package workledger

import (
	"errors"
	"fmt"
	"regexp"
)

// AgentBinding is the source-neutral agent identity a WorkItem is bound to. It
// is the Stage-5 foundation of the agent-platform-coupling design: durable
// identity that lets a later review comment correlate back to an AgentType, and
// that caps causation chains against a single root work item.
//
// Every field here is PLATFORM/BROKER-AUTHORITATIVE. Emitters state a domain
// fact and name no agent; a deployment-owned dispatcher route selects
// (AgentType, mode); the broker resolves the active generation and records the
// authoritative PR correlation from its own authenticated pull.create. None of
// these values ever originate from agent-controlled data or from an emitter's
// admission request. The zero value is valid and means "no agent resolved yet",
// which is exactly the pre-agent behavior of the ledger.
type AgentBinding struct {
	// AgentType is the concrete type, kebab-case (e.g. "youknowme-curator").
	AgentType string
	// Mode is a per-type enumerated mode, snake_case (e.g. "reconcile"). A type
	// rejects modes it does not declare; that enumeration is deployment-owned
	// and enforced above this layer.
	Mode string
	// TypeContractRevision pins the AgentType / container-invocation contract
	// revision the route resolved against.
	TypeContractRevision string
	// ResolvedReleaseGeneration is the broker-assigned monotonic generation
	// number of the active release. 0 means unresolved.
	ResolvedReleaseGeneration int64
	// ResolvedReleaseDigest is the broker-derived immutable image digest of the
	// resolved release, recorded for the run record and terminal result.
	ResolvedReleaseDigest string
	// BrokerRunID is the broker-minted run identity the capability was issued
	// for. It is broker-authoritative, never a caller-asserted run id.
	BrokerRunID string
	// AuthoritativePRRepository and AuthoritativePRNumber are recorded ONLY from
	// a broker-authenticated pull.create side effect, never parsed from agent
	// output, branch names, or PR body markers.
	AuthoritativePRRepository string
	AuthoritativePRNumber     int64
}

// kebabIdentifier bounds AgentType (class snake_case, type kebab-case per the
// taxonomy). We accept the kebab-case concrete-type shape.
var (
	kebabIdentifier   = regexp.MustCompile(`^[a-z][a-z0-9-]{0,127}$`)
	snakeIdentifier   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,127}$`)
	imageDigestFormat = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	repoFormat        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

// Zero reports whether no agent has been bound. The zero binding is the valid
// pre-agent state and must round-trip unchanged.
func (binding AgentBinding) Zero() bool {
	return binding == AgentBinding{}
}

// Validate checks a NON-ZERO binding for internal consistency. It is only meant
// for bindings the platform is asserting; the zero binding is always valid and
// callers should gate on Zero() first. It does not consult any live system and
// is therefore runnable offline in CI from either side of the seam.
func (binding AgentBinding) Validate() error {
	if binding.Zero() {
		return nil
	}
	if !kebabIdentifier.MatchString(binding.AgentType) {
		return errors.New("agent_type must be a bounded kebab-case identifier")
	}
	if !snakeIdentifier.MatchString(binding.Mode) {
		return errors.New("agent mode must be a bounded snake_case identifier")
	}
	if binding.TypeContractRevision == "" || len(binding.TypeContractRevision) > 256 {
		return errors.New("type_contract_revision is required and bounded when an agent is bound")
	}
	if binding.ResolvedReleaseGeneration < 0 {
		return errors.New("resolved_release_generation must be non-negative")
	}
	// A resolved generation and its digest travel together: monotonicity is by
	// generation number, enforcement is digest-only at launch.
	if (binding.ResolvedReleaseGeneration == 0) != (binding.ResolvedReleaseDigest == "") {
		return errors.New("resolved release generation and digest must be set together")
	}
	if binding.ResolvedReleaseDigest != "" && !imageDigestFormat.MatchString(binding.ResolvedReleaseDigest) {
		return errors.New("resolved_release_digest must be a sha256 image digest")
	}
	if binding.BrokerRunID != "" && len(binding.BrokerRunID) > 256 {
		return errors.New("broker_run_id is oversized")
	}
	// PR correlation is all-or-nothing and never partially asserted.
	if (binding.AuthoritativePRRepository == "") != (binding.AuthoritativePRNumber == 0) {
		return errors.New("authoritative PR repository and number must be set together")
	}
	if binding.AuthoritativePRRepository != "" {
		if !repoFormat.MatchString(binding.AuthoritativePRRepository) || len(binding.AuthoritativePRRepository) > 256 {
			return errors.New("authoritative_pr_repository must be owner/name")
		}
		if binding.AuthoritativePRNumber < 0 {
			return errors.New("authoritative_pr_number must be non-negative")
		}
	}
	return nil
}

// admissionSelectionFields enumerates the WorkItem fields a caller must NOT be
// able to supply at admission. Encoding it as data (rather than an ad-hoc set
// of if-statements) lets the offline validator and the admission guard share
// one source of truth for the invariant.
//
// The invariant: "emitters and runtime callers never select an image, release,
// or generation." At admission an event carries a domain fact only; the broker
// resolves the release generation and its digest later, and records the PR
// correlation from its own authenticated call. So a non-zero value in any of
// these fields on an admission request is a rejected selection attempt.
var admissionSelectionFields = []string{
	"resolved_release_generation",
	"resolved_release_digest",
	"broker_run_id",
	"authoritative_pr_repository",
	"authoritative_pr_number",
}

// AdmissionSelectionFields returns the field names an admission caller may not
// populate. Exposed so an offline contract validator can assert the set.
func AdmissionSelectionFields() []string {
	out := make([]string, len(admissionSelectionFields))
	copy(out, admissionSelectionFields)
	return out
}

// RejectAdmissionSelection encodes, offline, that a caller/emitter cannot supply
// image or generation selection fields (nor broker-authoritative PR
// correlation) at admission. AgentType/mode MAY be present at admission because
// a deployment-owned route selects them before the work item is created; what
// is forbidden is naming a release, generation, image digest, broker run, or PR
// correlation, all of which are resolved by the broker after admission.
//
// It returns a non-nil error naming the offending field when the binding
// carries any post-admission (broker-resolved) value, so the same check is
// usable both as an admission guard and as a CI contract assertion.
func (binding AgentBinding) RejectAdmissionSelection() error {
	violations := map[string]bool{
		"resolved_release_generation": binding.ResolvedReleaseGeneration != 0,
		"resolved_release_digest":     binding.ResolvedReleaseDigest != "",
		"broker_run_id":               binding.BrokerRunID != "",
		"authoritative_pr_repository": binding.AuthoritativePRRepository != "",
		"authoritative_pr_number":     binding.AuthoritativePRNumber != 0,
	}
	for _, field := range admissionSelectionFields {
		if violations[field] {
			return fmt.Errorf("admission caller may not supply %s: image, release, generation, broker run, and PR correlation are broker-resolved after admission", field)
		}
	}
	return nil
}

// AdmissionBinding is the subset of an AgentBinding a route may carry into
// admission: the deployment-owned (AgentType, mode) selection and the contract
// revision it resolved against. It deliberately cannot express a release,
// generation, image, broker run, or PR correlation.
func (binding AgentBinding) AdmissionBinding() AgentBinding {
	return AgentBinding{
		AgentType:            binding.AgentType,
		Mode:                 binding.Mode,
		TypeContractRevision: binding.TypeContractRevision,
	}
}
