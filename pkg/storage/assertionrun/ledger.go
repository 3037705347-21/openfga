// Package assertionrun defines the storage contract for assertion run batches.
//
// An assertion run "batch" pins, once and for all, the exact authorization
// state that its assertions are evaluated against:
//
//   - the authorization model used, and
//   - the relationship change position (an append-only changelog ULID).
//
// Every evaluation of the batch (the first run, a resume after an
// interruption, or an explicit replay) must use that same boundary, so tuples
// written after the batch was created can never leak into the results. The
// batch also records one immutable result per assertion and a single
// completion snapshot, which makes it possible to attribute a report to a
// concrete permission state and to diff two completed batches.
//
// This contract is intentionally separate from the legacy assertions backend:
// the existing WriteAssertions/ReadAssertions interfaces are left untouched,
// and stores are not required to implement this backend (see the standalone
// in-memory implementation).
package assertionrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// BatchStatus is the lifecycle state of an assertion run batch.
type BatchStatus string

const (
	// StatusRunning means the batch has been pinned but not every assertion has
	// a terminal result yet (including the case of an interrupted run).
	StatusRunning BatchStatus = "RUNNING"

	// StatusCompleted means every assertion has a terminal result and a single
	// completion snapshot has been recorded.
	StatusCompleted BatchStatus = "COMPLETED"
)

// ResultStatus is the terminal outcome of a single assertion within a batch.
type ResultStatus string

const (
	// ResultMatched means the check result agreed with the expectation.
	ResultMatched ResultStatus = "MATCHED"
	// ResultMismatched means the check result disagreed with the expectation.
	ResultMismatched ResultStatus = "MISMATCHED"
	// ResultError means the check could not be evaluated. Such results are
	// retried when a batch is resumed (but not when replayed).
	ResultError ResultStatus = "ERROR"
)

// Common storage errors.
var (
	// ErrBatchNotFound is returned when a batch id does not exist for a store.
	ErrBatchNotFound = errors.New("assertion run batch not found")

	// ErrBatchAlreadyExists is returned when a batch with the same id is
	// initialized twice.
	ErrBatchAlreadyExists = errors.New("assertion run batch already exists")

	// ErrBatchNotCompleted is returned when an operation requires a completed
	// batch (e.g. comparing reports) but the batch has not finished yet.
	ErrBatchNotCompleted = errors.New("assertion run batch not completed")
)

// Boundary pins the exact authorization state a batch is evaluated against.
//
// AuthorizationModelID is the model used for every check in the batch.
// ChangeULID is the ULID of the newest relationship change included in the
// batch: only tuples whose change ULID is less than or equal to it are
// visible. An empty ChangeULID means the batch was pinned against an empty
// changelog.
type Boundary struct {
	StoreID              string    `json:"store_id"`
	AuthorizationModelID string    `json:"authorization_model_id"`
	ChangeULID           string    `json:"change_ulid"`
	CapturedAt           time.Time `json:"captured_at"`
}

// Batch is the immutable header describing one assertion run. The boundary is
// fixed at creation and never changes, including across resumes and replays.
type Batch struct {
	ID                   string      `json:"id"`
	StoreID              string      `json:"store_id"`
	Name                 string      `json:"name,omitempty"`
	Boundary             Boundary    `json:"boundary"`
	Status               BatchStatus `json:"status"`
	TotalAssertions      int         `json:"total_assertions"`
	AssertionFingerprint string      `json:"assertion_fingerprint"`
	CreatedAt            time.Time   `json:"created_at"`
	UpdatedAt            time.Time   `json:"updated_at"`
	CompletedAt          time.Time   `json:"completed_at,omitempty"`
}

// Result records the outcome of evaluating the assertion at a fixed index in
// the batch. The assertion itself is stored alongside the result so that the
// report stays meaningful even if the live assertion set is overwritten later.
type Result struct {
	BatchID    string               `json:"batch_id"`
	Index      int                  `json:"index"`
	Assertion  *openfgav1.Assertion `json:"assertion"`
	Expected   bool                 `json:"expected"`
	Got        bool                 `json:"got"`
	Status     ResultStatus         `json:"status"`
	Error      string               `json:"error,omitempty"`
	Attempts   int                  `json:"attempts"`
	DurationMS int64                `json:"duration_ms"`
	UpdatedAt  time.Time            `json:"updated_at"`
}

// CompletionSnapshot is the single, authoritative summary written once when
// (and every time) a batch reaches completion.
type CompletionSnapshot struct {
	BatchID         string      `json:"batch_id"`
	StoreID         string      `json:"store_id"`
	Boundary        Boundary    `json:"boundary"`
	Status          BatchStatus `json:"status"`
	TotalAssertions int         `json:"total_assertions"`
	Matched         int         `json:"matched"`
	Mismatched      int         `json:"mismatched"`
	Errored         int         `json:"errored"`
	// Fingerprint is a deterministic hash of every per-assertion outcome. Two
	// runs against the same boundary produce the same fingerprint if and only if
	// they reached the same results.
	Fingerprint string    `json:"fingerprint"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMS  int64     `json:"duration_ms"`
}

// Report is the self-contained artifact of a finished batch: its header,
// every per-assertion result (ordered by index) and the completion snapshot.
type Report struct {
	Batch    *Batch              `json:"batch"`
	Results  []*Result           `json:"results"`
	Snapshot *CompletionSnapshot `json:"report,omitempty"`
}

// LedgerBackend stores assertion run batches and their results.
//
// It is an optional, standalone capability: datastores are not required to
// implement it, so adding it does not disturb the existing SQL backends or the
// legacy [storage.AssertionsBackend].
type LedgerBackend interface {
	// InitBatch persists a new batch together with an immutable copy of the
	// assertions it will run. It returns ErrBatchAlreadyExists if the batch id
	// is already taken.
	InitBatch(ctx context.Context, batch *Batch, assertions []*openfgav1.Assertion) (*Batch, error)

	// UpdateBatch persists mutations to the batch header (status/timestamps).
	// It returns ErrBatchNotFound if the batch does not exist for the store.
	UpdateBatch(ctx context.Context, store, batchID string, mutate func(*Batch) error) (*Batch, error)

	// GetBatch returns the batch header for a store. It returns
	// ErrBatchNotFound when missing.
	GetBatch(ctx context.Context, store, batchID string) (*Batch, error)

	// ListBatches returns all batches for a store ordered from newest to oldest
	// by creation time.
	ListBatches(ctx context.Context, store string) ([]*Batch, error)

	// GetAssertions returns the immutable, pinned assertions of a batch in
	// index order.
	GetAssertions(ctx context.Context, store, batchID string) ([]*openfgav1.Assertion, error)

	// PutResult upserts the result for a single (batch, assertion index).
	PutResult(ctx context.Context, store, batchID string, result *Result) error

	// GetResults returns the stored results for a batch ordered by index.
	// Indices without a stored result are simply absent.
	GetResults(ctx context.Context, store, batchID string) ([]*Result, error)
}

// AssertionKey is the stable identity of an assertion within a batch, used to
// align results when diffing two reports.
func AssertionKey(a *openfgav1.Assertion) string {
	tk := a.GetTupleKey()
	return tk.GetObject() + "#" + tk.GetRelation() + "@" + tk.GetUser()
}

// FingerprintAssertions returns a stable hash of an ordered assertion set so a
// batch can record exactly which assertions it was pinned to.
func FingerprintAssertions(assertions []*openfgav1.Assertion) string {
	h := sha256.New()
	for _, a := range assertions {
		tk := a.GetTupleKey()
		writeLine(h, tk.GetObject())
		writeLine(h, tk.GetRelation())
		writeLine(h, tk.GetUser())
		writeLine(h, boolToken(a.GetExpectation()))
		for _, ct := range a.GetContextualTuples() {
			writeLine(h, ct.GetObject())
			writeLine(h, ct.GetRelation())
			writeLine(h, ct.GetUser())
			writeLine(h, ct.GetCondition().GetName())
		}
		if a.GetContext() != nil {
			if b, err := proto.Marshal(a.GetContext()); err == nil {
				writeLine(h, string(b))
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// FingerprintResults returns a stable hash of the per-assertion outcomes in
// index order. Two runs over the same boundary share this fingerprint exactly
// when their outcomes are identical.
func FingerprintResults(results []*Result) string {
	h := sha256.New()
	for _, r := range results {
		writeLine(h, AssertionKey(r.Assertion))
		writeLine(h, boolToken(r.Expected))
		writeLine(h, boolToken(r.Got))
		writeLine(h, string(r.Status))
		writeLine(h, r.Error)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func boolToken(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// writeLine writes a length-prefixed token so that concatenated fields cannot
// be ambiguous.
func writeLine(h interface{ Write([]byte) (int, error) }, s string) {
	_, _ = h.Write([]byte(s))
	_, _ = h.Write([]byte{'\n'})
}
