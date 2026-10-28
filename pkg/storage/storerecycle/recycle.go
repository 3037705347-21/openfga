// Package storerecycle defines the storage contract for the store recycle bin
// and for staged, verified store recovery.
//
// Lifecycle model:
//
//   - Deleting a store captures an immutable [Snapshot] of every authorization
//     model version, every relationship tuple (including its condition) and
//     every non-empty assertion set, together with a [Manifest] of counts and
//     deterministic fingerprints. The captured store is then purged from the
//     live datastore and the snapshot is held as an [Entry] in state RECYCLED
//     until its retention expires.
//   - Recovering a store copies the snapshot into a staging datastore that is
//     physically separate from the live one. The staged content is re-read and
//     checked against the manifest before anything is published. A recovery is
//     tracked by a [RecoverySession] whose progress is persisted while it runs.
//   - Only after verification succeeds is the verified snapshot atomically
//     installed under the original store id. Failed or interrupted attempts
//     discard their staging datastore: half-copied data is never reachable
//     under the original id. On process restart, non-terminal sessions are
//     swept to DISCARDED and a fresh recovery can be started.
//   - Expired entries may be purged only when no recovery is active for them.
//
// This contract is intentionally standalone: datastores are not required to
// implement [Backend], the legacy [storage.StoresBackend] interface is left
// untouched, and neither the SQL backends nor the generated mocks need to
// change. See the standalone in-memory implementation.
//
// [storage.StoresBackend]: https://pkg.go.dev/github.com/openfga/openfga/pkg/storage#StoresBackend
package storerecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// EntryStatus is the lifecycle state of a recycled store entry.
type EntryStatus string

const (
	// EntryRecycled means the store is held in the recycle bin and may be
	// recovered or, once expired, purged.
	EntryRecycled EntryStatus = "RECYCLED"
	// EntryRecovering means a recovery session is currently active for the
	// entry. It must not be re-recycled or purged while in this state.
	EntryRecovering EntryStatus = "RECOVERING"
	// EntryRestored means a verified takeover completed and the store is live
	// again under its original id. The heavy snapshot payload is released; the
	// entry header is kept for audit.
	EntryRestored EntryStatus = "RESTORED"
	// EntryPurged means the entry was permanently removed. The value exists for
	// transition documentation only; purged entries are not stored.
	EntryPurged EntryStatus = "PURGED"
)

// RecoveryStatus is the state of a single recovery attempt.
type RecoveryStatus string

const (
	// RecoveryRunning is the only non-terminal state: the attempt is in COPY,
	// VERIFY or TAKEOVER and its staging datastore must not be trusted after a
	// process restart.
	RecoveryRunning RecoveryStatus = "RUNNING"
	// RecoveryCompleted means verification passed and the atomic takeover
	// succeeded.
	RecoveryCompleted RecoveryStatus = "COMPLETED"
	// RecoveryFailed means the attempt terminated with an error (other than a
	// crash). The staging datastore was discarded and the entry returned to
	// RECYCLED, so a new attempt can be made.
	RecoveryFailed RecoveryStatus = "FAILED"
	// RecoveryDiscarded means a non-terminal attempt was abandoned by the
	// startup sweep (e.g. after a restart).
	RecoveryDiscarded RecoveryStatus = "DISCARDED"
)

// Terminal reports whether the status can no longer change.
func (s RecoveryStatus) Terminal() bool {
	return s == RecoveryCompleted || s == RecoveryFailed || s == RecoveryDiscarded
}

// RecoveryPhase is the pipeline phase of a recovery attempt.
type RecoveryPhase string

const (
	// PhaseCopy copies the snapshot into the isolated staging datastore.
	PhaseCopy RecoveryPhase = "COPY"
	// PhaseVerify re-reads the staged content and checks it against the
	// manifest.
	PhaseVerify RecoveryPhase = "VERIFY"
	// PhaseTakeover installs the verified snapshot under the original store id.
	PhaseTakeover RecoveryPhase = "TAKEOVER"
)

// TakeoverResult is the outcome of reaching the TAKEOVER phase.
type TakeoverResult string

const (
	TakeoverSucceeded TakeoverResult = "SUCCESS"
	TakeoverFailed    TakeoverResult = "FAILED"
)

// Sentinel errors. Callers must match them with errors.Is.
var (
	// ErrEntryNotFound is returned when no recycle entry exists for a store.
	ErrEntryNotFound = errors.New("recycle entry not found")

	// ErrEntryAlreadyExists is returned when a new entry is created while a
	// conflicting non-terminal generation already exists.
	ErrEntryAlreadyExists = errors.New("recycle entry already exists")

	// ErrEntryNotRecycled is returned when a recovery is started for an entry
	// that is not in the RECYCLED state (e.g. already RESTORED).
	ErrEntryNotRecycled = errors.New("recycle entry is not in the RECYCLED state")

	// ErrRecoveryActive is returned when an entry is re-recycled or another
	// recovery is started while a non-terminal session exists.
	ErrRecoveryActive = errors.New("a recovery is already active for the store")

	// ErrRecoveryNotFound is returned when a recovery session id does not
	// exist for the store.
	ErrRecoveryNotFound = errors.New("recovery session not found")

	// ErrSnapshotUnavailable is returned when the heavy payload of an entry is
	// no longer held, e.g. after the entry was RESTORED or PURGED.
	ErrSnapshotUnavailable = errors.New("recycle snapshot unavailable")

	// ErrStoreAlreadyActive is returned by a takeover when the original store
	// id is already occupied by a live (non-recycled) store.
	ErrStoreAlreadyActive = errors.New("store id is already in use")

	// ErrVerificationFailed is the wrapped marker for manifest mismatches
	// detected during VERIFY. Match it with errors.Is; inspect the concrete
	// [VerificationMismatchError] for the per-kind detail.
	ErrVerificationFailed = errors.New("staged content failed verification")

	// ErrRecoveryNotActive is returned when a transition requires a
	// non-terminal session and none exists.
	ErrRecoveryNotActive = errors.New("recovery session is not active")
)

// Manifest is the integrity checklist captured with every snapshot.
type Manifest struct {
	ModelsCount         int       `json:"models_count"`
	ModelsFingerprint   string    `json:"models_fingerprint"`
	LatestModelID       string    `json:"latest_model_id"`
	TuplesCount         int       `json:"tuples_count"`
	TuplesFingerprint   string    `json:"tuples_fingerprint"`
	AssertionModels     int       `json:"assertion_models"`
	AssertionsCount     int       `json:"assertions_count"`
	AssertionsFingerprint string  `json:"assertions_fingerprint"`
	CapturedAt          time.Time `json:"captured_at"`
}

// Snapshot is the immutable, deep-copied content of a recycled store.
type Snapshot struct {
	// Store is the original store header (id, name, created_at, updated_at).
	Store *openfgav1.Store `json:"store"`
	// Models holds every authorization model version, newest id first is not
	// required: fingerprints are order-independent.
	Models []*openfgav1.AuthorizationModel `json:"models"`
	// Tuples holds every relationship tuple key, including conditions.
	Tuples []*openfgav1.TupleKey `json:"tuples"`
	// Assertions maps model id to its non-empty assertion set. Models without
	// assertions are absent from the map.
	Assertions map[string][]*openfgav1.Assertion `json:"assertions"`
	// Manifest is the counts-and-fingerprints checklist for the content above.
	Manifest Manifest `json:"manifest"`
}

// Entry is the header of one generation of a recycled store. The heavy
// [Snapshot] payload is never embedded here: read it explicitly through
// [Backend.GetSnapshot] while the entry is RECYCLED or RECOVERING.
type Entry struct {
	ID        string      `json:"id"`
	StoreID   string      `json:"store_id"`
	StoreName string      `json:"store_name"`
	Status    EntryStatus `json:"status"`

	DeletedAt time.Time `json:"deleted_at"`
	ExpiresAt time.Time `json:"expires_at"`

	// Manifest is retained even after RESTORED, so the header stays a useful
	// audit record.
	Manifest Manifest `json:"manifest"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Progress is the persisted progress of a recovery attempt.
type Progress struct {
	Phase RecoveryPhase `json:"phase"`

	ModelsTotal        int `json:"models_total"`
	ModelsCopied       int `json:"models_copied"`
	ModelsVerified     int `json:"models_verified"`
	TuplesTotal        int `json:"tuples_total"`
	TuplesCopied       int `json:"tuples_copied"`
	TuplesVerified     int `json:"tuples_verified"`
	AssertionModelsTotal    int `json:"assertion_models_total"`
	AssertionModelsCopied   int `json:"assertion_models_copied"`
	AssertionModelsVerified int `json:"assertion_models_verified"`
	AssertionsTotal    int `json:"assertions_total"`
	AssertionsCopied   int `json:"assertions_copied"`
	AssertionsVerified int `json:"assertions_verified"`
}

// RecoverySession tracks one recovery attempt.
type RecoverySession struct {
	ID             string `json:"id"`
	EntryID        string `json:"entry_id"`
	StoreID        string `json:"store_id"`
	StagingStoreID string `json:"staging_store_id"`
	Attempt        int    `json:"attempt"`

	Status     RecoveryStatus `json:"status"`
	Progress   Progress       `json:"progress"`
	FailReason string         `json:"fail_reason,omitempty"`

	StartedAt    time.Time `json:"started_at"`
	HeartbeatAt  time.Time `json:"heartbeat_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	CompletedAt  time.Time `json:"completed_at,omitempty"`
	DiscardReason string   `json:"discard_reason,omitempty"`
}

// TakeoverRecord is an audit record of an attempt that reached TAKEOVER.
type TakeoverRecord struct {
	ID             string          `json:"id"`
	StoreID        string          `json:"store_id"`
	SessionID      string          `json:"session_id"`
	StagingStoreID string          `json:"staging_store_id"`
	Result         TakeoverResult  `json:"result"`
	Manifest       Manifest        `json:"manifest"`
	Error          string          `json:"error,omitempty"`
	StartedAt      time.Time       `json:"started_at"`
	CompletedAt    time.Time       `json:"completed_at"`
}

// VerificationMismatch describes one failed manifest comparison.
type VerificationMismatch struct {
	// Kind is "models", "tuples" or "assertions".
	Kind string `json:"kind"`
	// Property is "count", "fingerprint" or "latest_model".
	Property string `json:"property"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
}

func (m VerificationMismatch) String() string {
	return m.Kind + " " + m.Property + ": expected " + m.Expected + ", got " + m.Actual
}

// VerificationMismatchError wraps one or more manifest mismatches and unwraps
// to [ErrVerificationFailed].
type VerificationMismatchError struct {
	Mismatches []VerificationMismatch
}

func (e *VerificationMismatchError) Error() string {
	parts := make([]string, 0, len(e.Mismatches))
	for _, m := range e.Mismatches {
		parts = append(parts, m.String())
	}
	return ErrVerificationFailed.Error() + ": " + strings.Join(parts, "; ")
}

// Unwrap enables errors.Is(err, ErrVerificationFailed).
func (e *VerificationMismatchError) Unwrap() error {
	return ErrVerificationFailed
}

// Backend stores recycle entries, recovery sessions and takeover records.
//
// All state transitions that must be atomic (single-flight gating, session
// and entry transitions, expiry gating) happen inside backend critical
// sections. Implementations must return deep copies so callers can never
// mutate stored state through returned pointers.
type Backend interface {
	// PutEntry creates a new generation of a recycled store entry and stores
	// its snapshot payload. It returns ErrRecoveryActive if the current entry
	// is RECOVERING. A RESTORED generation may be replaced by a new one; prior
	// takeover records are retained.
	PutEntry(ctx context.Context, entry *Entry, snapshot *Snapshot) error

	// GetEntry returns the current entry header for the store, or
	// ErrEntryNotFound. The snapshot payload is not included.
	GetEntry(ctx context.Context, storeID string) (*Entry, error)

	// GetSnapshot returns the deep-copied snapshot payload of the current
	// entry. It returns ErrSnapshotUnavailable for RESTORED/PURGED entries.
	GetSnapshot(ctx context.Context, storeID string) (*Snapshot, error)

	// ListEntries returns every known entry header, ordered by store id.
	ListEntries(ctx context.Context) ([]*Entry, error)

	// InitRecovery is the single-flight gate: it requires the entry to be
	// RECYCLED with no non-terminal session. On success it moves the entry to
	// RECOVERING and appends the session (which must already carry its id and
	// timestamps). It returns ErrRecoveryActive when another attempt is
	// running and ErrEntryNotFound when there is nothing to recover.
	InitRecovery(ctx context.Context, storeID string, session *RecoverySession) (*RecoverySession, error)

	// UpdateRecovery persists mutations of a non-terminal session under the
	// backend lock. It returns ErrRecoveryNotFound for unknown sessions and
	// ErrRecoveryNotActive for terminal ones.
	UpdateRecovery(ctx context.Context, storeID, sessionID string, mutate func(*RecoverySession) error) (*RecoverySession, error)

	// CompleteRecovery marks the session COMPLETED, releases the snapshot
	// payload and moves the entry to RESTORED in one critical section.
	CompleteRecovery(ctx context.Context, storeID, sessionID string, at time.Time) (*RecoverySession, error)

	// FailRecovery marks the session FAILED with a reason and moves the entry
	// back to RECYCLED in one critical section.
	FailRecovery(ctx context.Context, storeID, sessionID, reason string, at time.Time) (*RecoverySession, error)

	// DiscardStale moves every non-terminal session to DISCARDED with the
	// given reason and returns its entry to RECYCLED. It returns the affected
	// sessions (empty when there was nothing stale).
	DiscardStale(ctx context.Context, reason string, at time.Time) ([]*RecoverySession, error)

	// GetRecovery returns one session of a store, or ErrRecoveryNotFound.
	GetRecovery(ctx context.Context, storeID, sessionID string) (*RecoverySession, error)

	// ListRecoveries returns every session of the store, newest first.
	ListRecoveries(ctx context.Context, storeID string) ([]*RecoverySession, error)

	// AppendTakeover persists an audit record of a TAKEOVER attempt.
	AppendTakeover(ctx context.Context, record *TakeoverRecord) error

	// ListTakeovers returns takeover records of a store, oldest first.
	ListTakeovers(ctx context.Context, storeID string) ([]*TakeoverRecord, error)

	// PurgeExpired permanently removes entries that simultaneously satisfy:
	// status RECYCLED, ExpiresAt strictly before now, and no non-terminal
	// session for the store. Everything else (including expired RECOVERING
	// entries and RESTORED headers) is retained. Takeover records survive the
	// purge. It returns the purged entry headers.
	PurgeExpired(ctx context.Context, now time.Time) ([]*Entry, error)
}

// StorePurger is an optional datastore capability that hard-removes every row
// of a store: the store header along with its models, tuples, changelog and
// assertions. Datastores that do not implement it keep using the legacy
// soft-delete via [storage.StoresBackend.DeleteStore].
type StorePurger interface {
	PurgeStore(ctx context.Context, id string) error
}

// InstallPayload is the verified content installed under the original store
// id during TAKEOVER.
type InstallPayload struct {
	Store      *openfgav1.Store
	Models     []*openfgav1.AuthorizationModel
	Tuples     []*openfgav1.TupleKey
	Assertions map[string][]*openfgav1.Assertion
}

// StoreInstaller is an optional datastore capability for atomic takeover.
// InstallStore must either publish the store header and all of its content in
// a single critical section, or publish nothing and return
// [storage.ErrCollision] when the store id is already occupied. Changelog
// history is intentionally not part of the payload.
type StoreInstaller interface {
	InstallStore(ctx context.Context, payload InstallPayload) error
}

// BuildManifest computes the counts and deterministic fingerprints of a
// snapshot's content and fills its Manifest. The snapshot content itself is
// not modified; callers should have deep-copied it beforehand.
func BuildManifest(snap *Snapshot, capturedAt time.Time) Manifest {
	latest := ""
	for _, m := range snap.Models {
		if m.GetId() > latest {
			latest = m.GetId()
		}
	}

	assertionModels := 0
	totalAssertions := 0
	for _, asserts := range snap.Assertions {
		if len(asserts) > 0 {
			assertionModels++
			totalAssertions += len(asserts)
		}
	}

	return Manifest{
		ModelsCount:           len(snap.Models),
		ModelsFingerprint:     FingerprintModels(snap.Models),
		LatestModelID:         latest,
		TuplesCount:           len(snap.Tuples),
		TuplesFingerprint:     FingerprintTuples(snap.Tuples),
		AssertionModels:       assertionModels,
		AssertionsCount:       totalAssertions,
		AssertionsFingerprint: FingerprintAssertions(snap.Assertions),
		CapturedAt:            capturedAt,
	}
}

// CloneSnapshot returns a deep copy with no shared protobuf state. Mutating
// either the input or the result can never affect the other side.
func CloneSnapshot(in *Snapshot) *Snapshot {
	if in == nil {
		return nil
	}

	out := &Snapshot{
		Store:      proto.Clone(in.Store).(*openfgav1.Store),
		Models:     make([]*openfgav1.AuthorizationModel, 0, len(in.Models)),
		Tuples:     make([]*openfgav1.TupleKey, 0, len(in.Tuples)),
		Assertions: make(map[string][]*openfgav1.Assertion, len(in.Assertions)),
		Manifest:   in.Manifest,
	}

	for _, m := range in.Models {
		out.Models = append(out.Models, proto.Clone(m).(*openfgav1.AuthorizationModel))
	}

	for _, tk := range in.Tuples {
		out.Tuples = append(out.Tuples, cloneTupleKey(tk))
	}

	for modelID, asserts := range in.Assertions {
		cloned := make([]*openfgav1.Assertion, 0, len(asserts))
		for _, a := range asserts {
			cloned = append(cloned, proto.Clone(a).(*openfgav1.Assertion))
		}
		out.Assertions[modelID] = cloned
	}

	return out
}

func cloneTupleKey(in *openfgav1.TupleKey) *openfgav1.TupleKey {
	if in == nil {
		return nil
	}
	out := &openfgav1.TupleKey{
		Object:   in.GetObject(),
		Relation: in.GetRelation(),
		User:     in.GetUser(),
	}
	if in.GetCondition() != nil {
		out.Condition = proto.Clone(in.GetCondition()).(*openfgav1.RelationshipCondition)
	}
	return out
}

// writeLine writes one length-delimited token so concatenated fields cannot be
// ambiguous.
func writeLine(h interface{ Write([]byte) (int, error) }, s string) {
	_, _ = h.Write([]byte(s))
	_, _ = h.Write([]byte{'\n'})
}

func marshalFingerprint(h interface{ Write([]byte) (int, error) }, m proto.Message) {
	if b, err := proto.Marshal(m); err == nil {
		writeLine(h, hex.EncodeToString(b))
	} else {
		writeLine(h, "")
	}
}

// FingerprintModels returns an order-independent, content-sensitive hash of a
// set of authorization model versions.
func FingerprintModels(models []*openfgav1.AuthorizationModel) string {
	lines := make([]string, 0, len(models))
	for _, m := range models {
		h := sha256.New()
		writeLine(h, m.GetId())
		writeLine(h, m.GetSchemaVersion())
		marshalFingerprint(h, m)
		lines = append(lines, hex.EncodeToString(h.Sum(nil)))
	}
	sort.Strings(lines)

	root := sha256.New()
	for _, l := range lines {
		writeLine(root, l)
	}
	return hex.EncodeToString(root.Sum(nil))
}

func tupleKeyLine(tk *openfgav1.TupleKey) string {
	h := sha256.New()
	writeLine(h, tk.GetObject())
	writeLine(h, tk.GetRelation())
	writeLine(h, tk.GetUser())
	if c := tk.GetCondition(); c != nil {
		writeLine(h, c.GetName())
		marshalFingerprint(h, c.GetContext())
	} else {
		writeLine(h, "")
		writeLine(h, "")
	}
	return hex.EncodeToString(h.Sum(nil))
}

// FingerprintTuples returns an order-independent, content-sensitive hash of a
// set of relationship tuple keys, including their conditions.
func FingerprintTuples(tuples []*openfgav1.TupleKey) string {
	lines := make([]string, 0, len(tuples))
	for _, tk := range tuples {
		lines = append(lines, tupleKeyLine(tk))
	}
	sort.Strings(lines)

	root := sha256.New()
	for _, l := range lines {
		writeLine(root, l)
	}
	return hex.EncodeToString(root.Sum(nil))
}

// AssertionKey is the stable identity of an assertion within a model.
func AssertionKey(a *openfgav1.Assertion) string {
	tk := a.GetTupleKey()
	return tk.GetObject() + "#" + tk.GetRelation() + "@" + tk.GetUser()
}

func assertionLine(a *openfgav1.Assertion) string {
	h := sha256.New()
	tk := a.GetTupleKey()
	writeLine(h, tk.GetObject())
	writeLine(h, tk.GetRelation())
	writeLine(h, tk.GetUser())
	if a.GetExpectation() {
		writeLine(h, "1")
	} else {
		writeLine(h, "0")
	}
	for _, ct := range a.GetContextualTuples() {
		writeLine(h, tupleKeyLine(ct))
	}
	if a.GetContext() != nil {
		marshalFingerprint(h, a.GetContext())
	} else {
		writeLine(h, "")
	}
	return hex.EncodeToString(h.Sum(nil))
}

// FingerprintAssertions returns an order-independent hash of assertions
// grouped by model id.
func FingerprintAssertions(assertionsByModel map[string][]*openfgav1.Assertion) string {
	modelIDs := make([]string, 0, len(assertionsByModel))
	for id := range assertionsByModel {
		modelIDs = append(modelIDs, id)
	}
	sort.Strings(modelIDs)

	root := sha256.New()
	for _, id := range modelIDs {
		writeLine(root, id)
		lines := make([]string, 0, len(assertionsByModel[id]))
		for _, a := range assertionsByModel[id] {
			lines = append(lines, assertionLine(a))
		}
		sort.Strings(lines)
		for _, l := range lines {
			writeLine(root, l)
		}
	}
	return hex.EncodeToString(root.Sum(nil))
}
