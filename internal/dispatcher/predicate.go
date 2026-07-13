package dispatcher

import (
	"encoding/json"
	"fmt"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

const (
	Repository = "grubbyhacker/apple-jobs-matcher"
	Profile    = "codex-issue-implement"
)

type Candidate struct {
	Repository  string
	IssueNumber int64
	DeliveryID  string
}

func (c Candidate) SemanticKey() string {
	return fmt.Sprintf("github-issue-implement:v1:%s:%d", c.Repository, c.IssueNumber)
}

// Select decodes only the fields required for dispatch. The original provider
// payload is never returned to or stored by the dispatcher.
func Select(signal envelope.Signal) (Candidate, string) {
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
	if event.Action != "labeled" || event.Repository.FullName != Repository {
		return Candidate{}, "repository_filtered"
	}
	if event.Label.Name != "agent:implement" {
		return Candidate{}, "label_filtered"
	}
	if event.Issue.Number <= 0 || event.Issue.State != "open" || len(event.Issue.PullRequest) != 0 || event.Sender.Login == "" {
		return Candidate{}, "issue_filtered"
	}
	return Candidate{Repository: Repository, IssueNumber: event.Issue.Number, DeliveryID: signal.Meta.SourceDeliveryID}, "accepted"
}
