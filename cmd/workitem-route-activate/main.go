// Command workitem-route-activate is the managed, idempotent entrypoint that
// installs a deployment-owned route snapshot into the work ledger via the
// existing Store.ActivateRoute API and prints the STORE-MINTED
// route_snapshot_id for the resolver table.
//
// It accepts a full RouteDefinition JSON file and the Executor descriptor
// (id/kind/version) ActivateRoute requires. It NEVER accepts a caller-supplied
// route_snapshot_id. Reruns of the same definition+executor are idempotent; a
// differing definition for the same route id fails closed unless
// --allow-supersede opts into the explicit transition. It holds no launcher,
// broker, or dispatcher.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/routeactivatecmd"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func main() {
	database := flag.String("database", "", "work-ledger SQLite path to activate the route into (required)")
	routePath := flag.String("route", "", "path to the deployment-owned RouteDefinition JSON file (required)")
	executorID := flag.String("executor-id", "", "executor id the route definition names (required)")
	executorKind := flag.String("executor-kind", string(workledger.ExecutorDeterministicTool), "executor kind: deterministic_tool or policy_evaluator")
	executorVersion := flag.String("executor-version", "", "executor version string (required)")
	allowSupersede := flag.Bool("allow-supersede", false, "explicitly permit retiring a DIFFERENT active snapshot for the same route id")
	flag.Parse()

	if *database == "" || *routePath == "" || *executorID == "" || *executorVersion == "" {
		fmt.Fprintln(os.Stderr, "workitem-route-activate: --database, --route, --executor-id, and --executor-version are required")
		flag.Usage()
		os.Exit(2)
	}

	routeJSON, err := os.ReadFile(*routePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workitem-route-activate: read route file: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "workitem-route-activate version %s\n", buildinfo.Version)

	outcome, err := routeactivatecmd.Run(context.Background(), routeactivatecmd.Options{
		DatabasePath:        *database,
		RouteDefinitionJSON: routeJSON,
		Executor: workledger.ExecutorDescriptor{
			ID:      *executorID,
			Kind:    workledger.ExecutorKind(*executorKind),
			Version: *executorVersion,
		},
		AllowSupersede: *allowSupersede,
	}, time.Now().UTC())
	if err != nil {
		fmt.Fprintf(os.Stderr, "workitem-route-activate: %v\n", err)
		os.Exit(1)
	}

	routeactivatecmd.WriteOutcome(os.Stdout, outcome)
}
