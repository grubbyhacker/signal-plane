package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRouteActivationsValidatedAtLoad proves shadow_admission.route_activations
// is validated at config LOAD, not deferred to dispatcher startup: a valid
// entry loads, and every malformed entry is rejected by Load.
func TestRouteActivationsValidatedAtLoad(t *testing.T) {
	base := `
shadow_admission:
  enabled: true
  database_path: /var/lib/signal-plane/github-task-dispatcher.db
  route_activations:
`
	routesTail := `
routes:
  - id: manual-local
    path: /manual
    source: manual
    publish_subject: signals.manual.local.test
`
	write := func(t *testing.T, entries string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(base+entries+routesTail), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}

	t.Run("valid entry loads", func(t *testing.T) {
		cfg, err := write(t, `
    - route_definition_path: /etc/signal-plane/resume-route.json
      executor_id: youknowme_upload_v1
      executor_kind: deterministic_tool
      executor_version: v1
`)
		if err != nil {
			t.Fatalf("valid route activation rejected: %v", err)
		}
		if len(cfg.ShadowAdmission.RouteActivations) != 1 {
			t.Fatalf("route activations = %#v", cfg.ShadowAdmission.RouteActivations)
		}
	})

	t.Run("empty executor_kind defaults are allowed", func(t *testing.T) {
		if _, err := write(t, `
    - route_definition_path: /etc/signal-plane/resume-route.json
      executor_id: youknowme_upload_v1
      executor_version: v1
`); err != nil {
			t.Fatalf("omitted executor_kind rejected: %v", err)
		}
	})

	for name, entry := range map[string]string{
		"blank path": `
    - route_definition_path: ""
      executor_id: x
      executor_version: v1
`,
		"relative path": `
    - route_definition_path: relative/route.json
      executor_id: x
      executor_version: v1
`,
		"blank executor_id": `
    - route_definition_path: /etc/route.json
      executor_id: ""
      executor_version: v1
`,
		"blank executor_version": `
    - route_definition_path: /etc/route.json
      executor_id: x
      executor_version: ""
`,
		"bad executor_kind": `
    - route_definition_path: /etc/route.json
      executor_id: x
      executor_kind: launcher
      executor_version: v1
`,
		"duplicate path": `
    - route_definition_path: /etc/route.json
      executor_id: a
      executor_version: v1
    - route_definition_path: /etc/route.json
      executor_id: b
      executor_version: v2
`,
	} {
		t.Run("reject "+name, func(t *testing.T) {
			_, err := write(t, entry)
			if err == nil {
				t.Fatalf("%s was accepted", name)
			}
			if !strings.Contains(err.Error(), "route_activations") {
				t.Fatalf("error did not name route_activations: %v", err)
			}
		})
	}
}
