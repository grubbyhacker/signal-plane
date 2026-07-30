package dispatcher

import (
	"encoding/json"
	"fmt"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

type Candidate struct {
	Repository  string
	IssueNumber int64
	DeliveryID  string
	RouteID     string
	Profile     string
}

func (c Candidate) SemanticKey() string {
	return fmt.Sprintf("repository-task:v1:%s:%s:issue:%d", c.RouteID, c.Repository, c.IssueNumber)
}

// Select decodes only the fields required for dispatch. The original provider
// payload is never returned to or stored by the dispatcher.
func Select(signal envelope.Signal, routes []config.RepositoryTaskRoute) (Candidate, string) {
	if signal.Meta.Source != "github" || signal.Meta.SourceEvent != "issues" || signal.Meta.SourceAction != "labeled" {
		return Candidate{}, "event_filtered"
	}
	if signal.Meta.SourceDeliveryID == "" {
		return Candidate{}, "missing_delivery_id"
	}
	var event struct {
		Action     string `json:"action"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
		Issue struct {
			Number      int64           `json:"number"`
			State       string          `json:"state"`
			PullRequest json.RawMessage `json:"pull_request"`
		} `json:"issue"`
		Label struct {
			Name string `json:"name"`
		} `json:"label"`
		Sender struct {
			Login string `json:"login"`
		} `json:"sender"`
	}
	if json.Unmarshal(signal.Payload, &event) != nil {
		return Candidate{}, "invalid_payload"
	}
	if event.Issue.Number <= 0 || event.Issue.State != "open" || len(event.Issue.PullRequest) != 0 || event.Sender.Login == "" {
		return Candidate{}, "issue_filtered"
	}
	var matched *config.RepositoryTaskRoute
	for i := range routes {
		route := &routes[i]
		if event.Repository.FullName == route.Repository && signal.Meta.SourceEvent == route.Event &&
			event.Action == route.Action && event.Label.Name == route.Label {
			if matched != nil {
				return Candidate{}, "ambiguous_route"
			}
			matched = route
		}
	}
	if matched == nil {
		return Candidate{}, "route_filtered"
	}
	return Candidate{
		Repository: event.Repository.FullName, IssueNumber: event.Issue.Number,
		DeliveryID: signal.Meta.SourceDeliveryID, RouteID: matched.ID, Profile: matched.Profile,
	}, "accepted"
}
