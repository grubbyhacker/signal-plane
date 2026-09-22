# Stage 5 (command): shadow-ingress executable wiring

Status: landed, disabled by default. Process glue for the already-merged shadow
stack — WorkItem agent fields (#61), shadow-admission seam (#62), shadow ingress
(#63), route resolver (#64). No launcher, broker call, legacy dispatcher
mutation, network listener, or production config.

## What this stage establishes

`cmd/workitem-shadow-ingress` + `internal/shadowingresscmd` — the executable
that loads config and serves the Unix socket.

- `shadowingresscmd.Build(ctx, cfg, catalog, logger)` constructs the stack:
  parse the deployment-owned route config → `routeresolver.New` (catalog-
  validated) → `workledger.Open` → verify every referenced route snapshot is
  active → `shadowadmit.New` → `shadowingress.NewServer`. Returns `(nil, nil)`
  when disabled.
- `shadowingresscmd.Run` builds then serves until ctx cancel.
- `cmd/workitem-shadow-ingress/main.go` loads config, installs a
  SIGINT/SIGTERM `signal.NotifyContext`, and calls `Run`; ctx cancel stops the
  socket serve gracefully and the socket file is removed.

## Fail-closed

An **enabled** ingress refuses to serve (returns an error, opens nothing) on:

- missing `database_path` or `route_config_path`;
- a route config that references a `route_snapshot_id` not active in the ledger
  (`Store.RouteSnapshotExists`);
- a mode the AgentType catalog does not declare (`routeresolver.New`);
- a non-absolute socket path or a group/world-writable socket directory
  (`shadowingress.NewServer` → `validateSocketPath`).

## Config

`shadow_admission.ingress` gains `route_config_path` (the deployment-owned route
table). Disabled by default; no network address.

## Invariant: no launcher/dispatcher/broker in the import closure

`scripts/validate-shadow-ingress-command.py` (wired into `mise run check` as
`validate:shadow-ingress-command`) computes the full transitive import set of
both command packages with `go list -deps` and fails if any
`internal/dispatcher` / `internal/recovery` / `internal/pushscan` package
appears — proving the executable cannot reach a launcher, the legacy dispatcher,
or a broker even transitively. It also asserts the command wires the shadow
stack (shadowadmit, shadowingress, routeresolver, workledger).

## Tests

`internal/shadowingresscmd/run_test.go`: disabled no-op; requires
db+route-config paths when enabled; Build+Serve lifecycle with ctx-cancel
graceful shutdown and socket cleanup; `Run` returns on cancel; fail-closed on
missing route snapshot, catalog mismatch, and bad socket permissions. Plus a
config test for `route_config_path`.

## Gate

`mise run check` — fmt, six offline validators (incl.
`validate:shadow-ingress-command`), `go test ./...`, build (now including
`./cmd/workitem-shadow-ingress`).
