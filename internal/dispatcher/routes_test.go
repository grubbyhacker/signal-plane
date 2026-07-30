package dispatcher

import "github.com/grubbyhacker/signal-plane/internal/config"

const (
	Repository = "example/automation-target"
	Profile    = "repository-task"
)

func testRepositoryTaskRoutes() []config.RepositoryTaskRoute {
	return []config.RepositoryTaskRoute{{
		ID:         "automation-target",
		Repository: Repository,
		Event:      "issues",
		Action:     "labeled",
		Label:      "automation:requested",
		Profile:    Profile,
	}}
}
