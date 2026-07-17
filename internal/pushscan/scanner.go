package pushscan

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

var (
	shaPattern    = regexp.MustCompile(`^[0-9a-f]{40}$`)
	canaryPattern = regexp.MustCompile(`(?i)PR10[_-]CREDENTIAL[_-]CANARY(?::|=|[_-])[A-Za-z0-9._~+/-]{6,}`)
	jwtPattern    = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`)
	tokenRun      = regexp.MustCompile(`[A-Za-z0-9._~+/-]{16,1000}`)
	base64Run     = regexp.MustCompile(`[A-Za-z0-9+/_-]{16,}={0,2}`)
	hexRun        = regexp.MustCompile(`(?i)(?:[0-9a-f]{2}){12,}`)
)

type Scanner struct {
	Store             *Store
	Broker            Broker
	EventSink         EventSink
	FingerprintKey    []byte
	Bounds            Bounds
	Profile           string
	ProfileGeneration int64
	CanaryAttribution Attribution
	Repositories      []string
	Refs              []string
	Clock             func() time.Time
}

type MaterialError struct{ Code string }

func (e MaterialError) Error() string { return "push scan material unavailable: " + e.Code }

func IdentityFromSignal(signal envelope.Signal, sequence uint64) (PushIdentity, error) {
	meta := signal.Meta
	if meta.Source != "github" || meta.SourceEvent != "push" || !meta.Authentication.Verified || meta.Authentication.Method != "github_hmac_sha256" || meta.SourceDeliveryID == "" || meta.Namespace == "" || !strings.HasPrefix(meta.SourceRef, "refs/heads/") || !shaPattern.MatchString(meta.SourceBefore) || !shaPattern.MatchString(meta.SourceAfter) || meta.ReceivedAt.IsZero() || sequence == 0 {
		return PushIdentity{}, errors.New("signed push identity is incomplete")
	}
	return PushIdentity{DeliveryID: meta.SourceDeliveryID, Repository: meta.Namespace, Ref: meta.SourceRef, Before: meta.SourceBefore, After: meta.SourceAfter, HeadTime: meta.SourceHeadTime, SourceTime: meta.SourceTimestamp, ReceivedAt: meta.ReceivedAt, StreamSequence: sequence}, nil
}

func (s *Scanner) Process(ctx context.Context, identity PushIdentity) (Result, error) {
	if s.Store == nil || s.Broker == nil || len(s.FingerprintKey) < 32 || s.Profile == "" || s.ProfileGeneration <= 0 {
		return Result{}, errors.New("push scanner dependencies are incomplete")
	}
	now := s.now()
	receiptAt, _, _, err := s.Store.RecordReceipt(ctx, identity, now)
	if err != nil {
		return Result{}, err
	}
	if existing, ok, err := s.Store.Result(ctx, identity.DeliveryID); err != nil || ok {
		if err != nil {
			return existing, err
		}
		return s.runSideEffects(ctx, identity, existing)
	}
	result := Result{DeliveryID: identity.DeliveryID, ScanStartedAt: now, ReceiptAt: receiptAt, Profile: s.Profile, ProfileGeneration: s.ProfileGeneration, SLOState: "not_applicable"}
	result.SLODeadline = receiptAt.Add(5 * time.Minute)
	if !contains(s.Repositories, identity.Repository) || !contains(s.Refs, identity.Ref) {
		result.Status, result.Severity, result.ReasonCode = "rejected_catalog", "none", "catalog_rejected"
		result.MaterialCompletedAt, result.TerminalAt = now, now
		if err := s.Store.RecordResult(ctx, result, nil); err != nil {
			return Result{}, err
		}
		return result, nil
	}
	request := MaterialRequest{Version: WireVersion, DeliveryID: identity.DeliveryID, Repository: identity.Repository, Ref: identity.Ref, Before: identity.Before, After: identity.After}
	material, materialErr := s.Broker.Material(ctx, request)
	result.MaterialCompletedAt = s.now()
	var finding Finding
	if materialErr != nil {
		code := "material_unavailable"
		var bounded MaterialError
		if errors.As(materialErr, &bounded) {
			code = safeReason(bounded.Code)
		}
		finding = s.highFinding(identity, code)
	} else if !exactMaterialIdentity(material, identity) {
		finding = s.highFinding(identity, "material_identity_mismatch")
	} else if !material.Complete {
		finding = s.highFinding(identity, safeReason(material.ReasonCode))
	} else {
		finding, err = s.scanMaterial(ctx, identity, material)
		if err != nil {
			finding = s.highFinding(identity, "scan_bound_exceeded")
		}
	}
	result.TerminalAt = s.now()
	if finding.Severity == "" {
		result.Status, result.Severity, result.ReasonCode = "clean", "none", "clean"
	} else {
		result.FindingID, result.Severity, result.ReasonCode = finding.ID, finding.Severity, finding.ReasonCode
		result.Status, result.FindingAt = "finding_"+finding.Severity, s.now()
		copyAttribution(&result, finding.Attribution)
		if !finding.Attribution.IssuedAt.IsZero() && finding.Attribution.ExpiresAt.After(finding.Attribution.IssuedAt) {
			ttlSLO := finding.Attribution.ExpiresAt.Sub(finding.Attribution.IssuedAt) / 10
			if ttlSLO < 5*time.Minute {
				result.SLODeadline = receiptAt.Add(ttlSLO)
			}
		}
		liveResponse := finding.Severity == "high" && (finding.Attribution.FingerprintID == "" || finding.Attribution.State == "active")
		if liveResponse {
			result.Status, result.ResponseRequestedAt, result.SLOState = "response_pending", s.now(), "pending"
		} else if finding.Severity == "high" && finding.Attribution.FingerprintID != "" {
			result.SLOState = "not_live_when_scanned"
		}
	}
	var event *SecurityEvent
	if finding.Severity != "" {
		result.AlertState, result.AlertRequestedAt = "alert_requested", s.now()
		eventValue := securityEvent(identity, result)
		event = &eventValue
	}
	if err := s.Store.RecordResult(ctx, result, event); err != nil {
		return Result{}, err
	}
	return s.runSideEffects(ctx, identity, result)
}

func (s *Scanner) runSideEffects(ctx context.Context, identity PushIdentity, result Result) (Result, error) {
	var sideEffectErrors []error
	if err := s.flushEvents(ctx); err != nil {
		sideEffectErrors = append(sideEffectErrors, fmt.Errorf("flush security event outbox: %w", err))
	}
	if result.Status == "response_pending" {
		completed, err := s.completeResponse(ctx, identity, result)
		if err != nil {
			sideEffectErrors = append(sideEffectErrors, fmt.Errorf("complete pending broker response: %w", err))
		} else {
			result = completed
		}
	}
	return result, errors.Join(sideEffectErrors...)
}

// Reconcile advances durable side effects without requiring another webhook or
// JetStream delivery. Alert publication and broker response are independent;
// one failure never prevents an attempt of the other.
func (s *Scanner) Reconcile(ctx context.Context) error {
	if s.Store == nil || s.Broker == nil {
		return errors.New("push scanner reconciliation dependencies are incomplete")
	}
	var reconciliationErrors []error
	if err := s.flushEvents(ctx); err != nil {
		reconciliationErrors = append(reconciliationErrors, fmt.Errorf("flush security event outbox: %w", err))
	}
	if err := s.Store.MarkOverdueResponses(ctx, s.now()); err != nil {
		reconciliationErrors = append(reconciliationErrors, fmt.Errorf("mark overdue responses: %w", err))
	}
	deliveryIDs, err := s.Store.PendingResponseDeliveryIDs(ctx)
	if err != nil {
		reconciliationErrors = append(reconciliationErrors, fmt.Errorf("list pending responses: %w", err))
		return errors.Join(reconciliationErrors...)
	}
	for _, deliveryID := range deliveryIDs {
		identity, identityErr := s.Store.PushIdentity(ctx, deliveryID)
		result, ok, resultErr := s.Store.Result(ctx, deliveryID)
		if identityErr != nil {
			reconciliationErrors = append(reconciliationErrors, fmt.Errorf("load pending response identity %s: %w", deliveryID, identityErr))
			continue
		}
		if resultErr != nil {
			reconciliationErrors = append(reconciliationErrors, fmt.Errorf("load pending response result %s: %w", deliveryID, resultErr))
			continue
		}
		if !ok {
			reconciliationErrors = append(reconciliationErrors, fmt.Errorf("load pending response result %s: result missing", deliveryID))
			continue
		}
		if _, err := s.completeResponse(ctx, identity, result); err != nil {
			reconciliationErrors = append(reconciliationErrors, fmt.Errorf("complete pending response %s: %w", deliveryID, err))
		}
	}
	return errors.Join(reconciliationErrors...)
}

func (s *Scanner) completeResponse(ctx context.Context, identity PushIdentity, result Result) (Result, error) {
	if err := s.Store.RecordResponseAttempt(ctx, result.DeliveryID, s.now()); err != nil {
		return result, err
	}
	attribution := Attribution{FingerprintID: result.FingerprintID, Profile: result.Profile, ProfileGeneration: result.ProfileGeneration, LogicalSessionID: result.LogicalSessionID, SessionLineageID: result.SessionLineageID, WorkerID: result.WorkerID, WorkerStorageLineage: result.WorkerStorageLineage, WorkerFenceEpoch: result.WorkerFenceEpoch}
	finding := Finding{ID: result.FindingID, Severity: result.Severity, ReasonCode: result.ReasonCode, Attribution: attribution}
	response, err := s.Broker.Respond(ctx, "push-tripwire:"+finding.ID, responseRequest(identity, finding))
	if err != nil {
		return result, err
	}
	halted, fenceRequested, fenced, fenceState, err := validateResponse(response, finding)
	if err != nil {
		return result, err
	}
	result.HaltedAt, result.FenceRequestedAt, result.FencedAt, result.FenceState = halted, fenceRequested, fenced, fenceState
	result.TerminalAt = maxTime(result.TerminalAt, halted, fenceRequested, fenced)
	result.SLOBreached = maxTime(result.AlertRequestedAt, result.HaltedAt).After(result.SLODeadline)
	if result.SLOBreached {
		result.SLOState = "breached"
	} else {
		result.SLOState = "met"
	}
	result.Status = "finding_" + result.Severity
	if err := s.Store.CompleteResponse(ctx, result); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Scanner) scanMaterial(ctx context.Context, identity PushIdentity, material Material) (Finding, error) {
	if len(material.Commits) > s.Bounds.MaxCommits || len(material.Files) > s.Bounds.MaxPaths || material.Bounds.CommitCount > s.Bounds.MaxCommits || material.Bounds.PathCount > s.Bounds.MaxPaths || material.Bounds.TotalBytes > s.Bounds.MaxTotalBytes {
		return Finding{}, errors.New("material bounds exceeded")
	}
	values := make([]string, 0, len(material.Commits)+len(material.Files)*2)
	var total int64
	for _, commit := range material.Commits {
		if !shaPattern.MatchString(commit.SHA) {
			return Finding{}, errors.New("invalid commit identity")
		}
		total += int64(len(commit.Message))
		if total > s.Bounds.MaxTotalBytes {
			return Finding{}, errors.New("total bytes exceeded")
		}
		values = append(values, commit.Message)
	}
	for _, file := range material.Files {
		if file.Path == "" || file.Size < 0 || file.Size > s.Bounds.MaxBlobBytes || !shaPattern.MatchString(file.CommitSHA) || !shaPattern.MatchString(file.BlobSHA) || (file.Side != "before" && file.Side != "after") {
			return Finding{}, errors.New("file bounds exceeded")
		}
		content, err := base64.StdEncoding.DecodeString(file.ContentBase64)
		if err != nil || int64(len(content)) != file.Size {
			return Finding{}, errors.New("invalid bounded blob")
		}
		total += int64(len(content))
		if total > s.Bounds.MaxTotalBytes {
			return Finding{}, errors.New("total bytes exceeded")
		}
		values = append(values, file.Path, string(content))
	}
	if material.Bounds.CommitCount != len(material.Commits) || material.Bounds.PathCount != len(material.Files) || material.Bounds.TotalBytes != total {
		return Finding{}, errors.New("material bound accounting mismatch")
	}
	return s.scanValues(ctx, identity, values)
}

type queuedValue struct {
	value string
	depth int
}

func (s *Scanner) scanValues(ctx context.Context, identity PushIdentity, inputs []string) (Finding, error) {
	queue := make([]queuedValue, 0, len(inputs))
	seen := map[string]struct{}{}
	add := func(value string, depth int) error {
		if value == "" {
			return nil
		}
		if _, ok := seen[value]; ok {
			return nil
		}
		if len(seen) >= s.Bounds.MaxCandidates {
			return errors.New("candidate bound exceeded")
		}
		seen[value] = struct{}{}
		queue = append(queue, queuedValue{value, depth})
		return nil
	}
	for _, value := range inputs {
		if err := add(value, 0); err != nil {
			return Finding{}, err
		}
	}
	low := Finding{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if canaryPattern.MatchString(current.value) {
			finding := s.highFinding(identity, "seeded_canary_match")
			finding.Attribution = s.CanaryAttribution
			return finding, nil
		}
		candidates := append([]string{current.value}, tokenRun.FindAllString(current.value, -1)...)
		for _, candidate := range candidates {
			if attribution, ok, err := s.Store.MatchCandidate(ctx, s.FingerprintKey, candidate, s.now()); err != nil {
				return Finding{}, err
			} else if ok {
				reason := "issued_token_fingerprint_match"
				if attribution.State != "active" {
					reason = attribution.State + "_token_fingerprint_match"
				}
				finding := s.highFinding(identity, reason)
				finding.Attribution = attribution
				finding.ID = findingID(identity, reason, attribution.FingerprintID)
				return finding, nil
			}
		}
		if low.Severity == "" && (jwtPattern.MatchString(current.value) || genericEntropyShape(current.value)) {
			low = lowFinding(identity, "generic_credential_shape", s.Profile, s.ProfileGeneration)
		}
		if current.depth >= s.Bounds.MaxDecodeDepth {
			if hasFullReversibleDecoding(current.value) {
				return Finding{}, errors.New("decode depth exceeded")
			}
			continue
		}
		for _, decoded := range reversibleDecodings(current.value) {
			if err := add(decoded, current.depth+1); err != nil {
				return Finding{}, err
			}
		}
	}
	return low, nil
}

func hasFullReversibleDecoding(value string) bool {
	if decoded, err := url.PathUnescape(value); err == nil && decoded != value {
		return true
	}
	if len(value) >= 24 && len(value)%2 == 0 {
		if _, err := hex.DecodeString(value); err == nil {
			return true
		}
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if len(value) >= 16 {
			if _, err := encoding.DecodeString(value); err == nil {
				return true
			}
		}
	}
	return false
}

func reversibleDecodings(value string) []string {
	result := []string{}
	if decoded, err := url.PathUnescape(value); err == nil && decoded != value {
		result = append(result, decoded)
	}
	for _, match := range hexRun.FindAllString(value, -1) {
		for start := 0; start < 2; start++ {
			for end := 0; end < 2; end++ {
				if len(match)-start-end < 24 || (len(match)-start-end)%2 != 0 {
					continue
				}
				if decoded, err := hex.DecodeString(match[start : len(match)-end]); err == nil {
					result = append(result, string(decoded))
				}
			}
		}
	}
	for _, match := range base64Run.FindAllString(value, -1) {
		for start := 0; start < 4; start++ {
			for end := 0; end < 4; end++ {
				if len(match)-start-end < 16 {
					continue
				}
				candidate := match[start : len(match)-end]
				for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
					if decoded, err := encoding.DecodeString(candidate); err == nil {
						result = append(result, string(decoded))
					}
				}
			}
		}
	}
	return result
}

func genericEntropyShape(value string) bool {
	for _, candidate := range tokenRun.FindAllString(value, -1) {
		if len(candidate) >= 32 && strings.ContainsAny(candidate, "0123456789") && strings.ContainsAny(candidate, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && strings.ContainsAny(candidate, "abcdefghijklmnopqrstuvwxyz") {
			return true
		}
	}
	return false
}

func exactMaterialIdentity(material Material, identity PushIdentity) bool {
	return material.Version == WireVersion && material.DeliveryID == identity.DeliveryID && material.Repository == identity.Repository && material.Ref == identity.Ref && material.Before == identity.Before && material.After == identity.After
}
func (s *Scanner) highFinding(identity PushIdentity, reason string) Finding {
	return Finding{ID: findingID(identity, reason, ""), Severity: "high", ReasonCode: reason, Attribution: Attribution{Profile: s.Profile, ProfileGeneration: s.ProfileGeneration}}
}
func lowFinding(identity PushIdentity, reason, profile string, generation int64) Finding {
	return Finding{ID: findingID(identity, reason, ""), Severity: "low", ReasonCode: reason, Attribution: Attribution{Profile: profile, ProfileGeneration: generation}}
}
func findingID(identity PushIdentity, reason, fingerprint string) string {
	digest := sha256.Sum256([]byte(identity.DeliveryID + "\x00" + reason + "\x00" + fingerprint))
	return "push-finding-" + hex.EncodeToString(digest[:16])
}
func safeReason(value string) string {
	switch value {
	case "before_mismatch", "ref_deletion_rejected", "non_fast_forward_rejected", "commit_bound_exceeded", "path_bound_exceeded", "blob_bound_exceeded", "total_bytes_exceeded", "candidate_bound_exceeded", "decode_bound_exceeded":
		return value
	default:
		return "material_incomplete"
	}
}
func copyAttribution(result *Result, attribution Attribution) {
	result.FingerprintID, result.Profile, result.LogicalSessionID, result.SessionLineageID, result.WorkerID, result.WorkerStorageLineage, result.WorkerFenceEpoch, result.ProfileGeneration = attribution.FingerprintID, attribution.Profile, attribution.LogicalSessionID, attribution.SessionLineageID, attribution.WorkerID, attribution.WorkerStorageLineage, attribution.WorkerFenceEpoch, attribution.ProfileGeneration
}
func responseRequest(identity PushIdentity, finding Finding) ResponseRequest {
	actions := []string{"halt_issuance"}
	if finding.Attribution.WorkerID != "" {
		actions = append(actions, "fence_worker_session")
	}
	sort.Strings(actions)
	a := finding.Attribution
	request := ResponseRequest{Version: WireVersion, FindingID: finding.ID, DeliveryID: identity.DeliveryID, Repository: identity.Repository, Ref: identity.Ref, Before: identity.Before, After: identity.After, Severity: finding.Severity, ReasonCode: finding.ReasonCode, FingerprintID: a.FingerprintID, Profile: a.Profile, ProfileGeneration: a.ProfileGeneration, Actions: actions}
	if a.WorkerID != "" {
		request.Binding = &ResponseBinding{LogicalSessionID: a.LogicalSessionID, SessionLineageID: a.SessionLineageID, WorkerID: a.WorkerID, WorkerStorageLineage: a.WorkerStorageLineage, WorkerFenceEpoch: a.WorkerFenceEpoch}
	}
	return request
}
func validateResponse(response ResponseResult, finding Finding) (time.Time, time.Time, time.Time, string, error) {
	if response.Version != WireVersion || response.FindingID != finding.ID {
		return time.Time{}, time.Time{}, time.Time{}, "", errors.New("broker response identity mismatch")
	}
	var halted, fenceRequested, fenced time.Time
	var fenceState string
	for _, action := range response.Actions {
		if action.CompletedAt.IsZero() {
			return time.Time{}, time.Time{}, time.Time{}, "", errors.New("broker action response invalid")
		}
		switch action.Action {
		case "halt_issuance":
			if action.State != "halted" {
				return time.Time{}, time.Time{}, time.Time{}, "", errors.New("broker halt response invalid")
			}
			halted = action.CompletedAt
		case "fence_worker_session":
			fenceState = action.State
			switch action.State {
			case "fence_requested":
				fenceRequested = action.CompletedAt
			case "fenced":
				fenceRequested, fenced = action.CompletedAt, action.CompletedAt
			default:
				return time.Time{}, time.Time{}, time.Time{}, "", errors.New("broker fence response invalid")
			}
		default:
			return time.Time{}, time.Time{}, time.Time{}, "", errors.New("broker returned unknown action")
		}
	}
	if halted.IsZero() || (finding.Attribution.WorkerID != "" && fenceRequested.IsZero()) {
		return time.Time{}, time.Time{}, time.Time{}, "", errors.New("broker omitted required response action")
	}
	return halted.UTC(), fenceRequested.UTC(), fenced.UTC(), fenceState, nil
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func securityEvent(identity PushIdentity, result Result) SecurityEvent {
	return SecurityEvent{Version: "signal/push-tripwire-event/v1", EventID: "push-event-" + result.FindingID, State: result.AlertState, FindingID: result.FindingID, DeliveryID: identity.DeliveryID, Repository: identity.Repository, Ref: identity.Ref, Before: identity.Before, After: identity.After, Severity: result.Severity, ReasonCode: result.ReasonCode, FingerprintID: result.FingerprintID, Profile: result.Profile, ProfileGeneration: result.ProfileGeneration, LogicalSessionID: result.LogicalSessionID, SessionLineageID: result.SessionLineageID, WorkerID: result.WorkerID, WorkerStorageLineage: result.WorkerStorageLineage, WorkerFenceEpoch: result.WorkerFenceEpoch, ReceivedAt: identity.ReceivedAt, ReceiptAt: result.ReceiptAt, FindingAt: result.FindingAt, AlertRequestedAt: result.AlertRequestedAt}
}
func (s *Scanner) flushEvents(ctx context.Context) error {
	events, err := s.Store.PendingEvents(ctx)
	if err != nil {
		return err
	}
	if s.EventSink == nil {
		return nil
	}
	for _, event := range events {
		if err := s.EventSink.Publish(ctx, event); err != nil {
			return err
		}
		if err := s.Store.MarkEventPublished(ctx, event.EventID, s.now()); err != nil {
			return err
		}
	}
	return nil
}
func maxTime(values ...time.Time) time.Time {
	var result time.Time
	for _, value := range values {
		if value.After(result) {
			result = value
		}
	}
	return result
}
func (s *Scanner) now() time.Time {
	if s.Clock != nil {
		return s.Clock().UTC()
	}
	return time.Now().UTC()
}
func SortedCandidates(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
func (f Finding) String() string { return fmt.Sprintf("%s:%s", f.Severity, f.ReasonCode) }
