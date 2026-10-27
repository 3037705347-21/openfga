package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// Named change consumers allow a pool of workers to collaboratively drain a
// store's changelog. The guarantees provided by [ChangeConsumerBackend] are:
//
//   - Generation-fenced lease: at most one holder may act for a consumer at any
//     time. Every holder change (takeover) is a generation switch; commits
//     carrying a stale generation are rejected.
//   - Durable progress: the ULID of the last fully delivered change is
//     persisted per consumer and survives worker restarts and lease
//     hand-offs.
//   - Complete-page receipts: every confirmed page produces an append-only
//     receipt chaining contiguously from the previous progress.
//
// All three pieces (lease/fence check, receipt write, progress advance) are
// committed in a single transaction boundary against the changelog, so reads,
// receipts, progress and legacy [ChangelogBackend.ReadChanges] pagination
// always agree on the same page boundaries.
var (
	// ErrChangeConsumerNotFound is returned when the named consumer does not
	// exist (it must be created with AcquireChangeLease first).
	ErrChangeConsumerNotFound = newChangeConsumerError("change consumer not found")

	// ErrChangeLeaseHeld is returned when a non-holder tries to acquire a
	// lease that is currently held and unexpired.
	ErrChangeLeaseHeld = newChangeConsumerError("change lease is held by another holder")

	// ErrChangeLeaseFenced is returned when the caller is not the current
	// lease holder: either the lease expired, a takeover switched the
	// generation, or the supplied generation does not match.
	ErrChangeLeaseFenced = newChangeConsumerError("change lease generation is stale or lease has expired")

	// ErrChangeConsumerScopeMismatch is returned when an acquire attempt pins
	// an object type filter that differs from the consumer's existing scope.
	ErrChangeConsumerScopeMismatch = newChangeConsumerError("change consumer subscription scope (object type) mismatch")

	// ErrInvalidPageReceipt is returned when a confirmation does not chain
	// contiguously from the persisted progress, confirms an empty page, or
	// regresses an already persisted receipt.
	ErrInvalidPageReceipt = newChangeConsumerError("invalid change page receipt")

	// ErrPageChecksumMismatch is returned when the page fingerprint supplied
	// by the worker does not match the changes still present in the changelog
	// for the receipt's (StartULID, EndULID] range.
	ErrPageChecksumMismatch = newChangeConsumerError("change page checksum does not match changelog contents")
)

// ChangeConsumerError is the wrapped error type for the sentinel errors above
// so callers can distinguish consumer-protocol failures via errors.As.
type ChangeConsumerError struct{ msg string }

func newChangeConsumerError(msg string) error { return &ChangeConsumerError{msg: msg} }

func (e *ChangeConsumerError) Error() string { return e.msg }

// ChangeLease is an exclusive, expiring grant for a named consumer.
type ChangeLease struct {
	Store      string
	Consumer   string
	Holder     string
	Generation uint64
	ObjectType string

	// AcquiredAt is when the current generation was granted. It is unchanged
	// by same-holder renewals.
	AcquiredAt time.Time
	// ExpiresAt is the current lease deadline. A takeover is possible only
	// after this instant.
	ExpiresAt time.Time
}

// ValidAt reports whether the lease is held by holder with generation at now.
func (l *ChangeLease) ValidAt(holder string, generation uint64, now time.Time) bool {
	return l != nil && l.Holder == holder && l.Generation == generation && now.Before(l.ExpiresAt)
}

// PageReceipt is the durable, verifiable record that one complete page
// (StartULID, EndULID] was delivered to and acknowledged by the consumer.
//
// Receipts form a contiguous chain: receipt N's StartULID equals receipt N-1's
// EndULID (and the consumer's persisted [ChangeConsumerState.ProgressULID]).
// The first receipt starts at the empty ULID.
type PageReceipt struct {
	Store      string
	Consumer   string
	ObjectType string

	// PageNumber is the 1-based position of the receipt in the chain.
	PageNumber uint64
	// Generation is the lease generation under which the page was confirmed.
	Generation uint64

	// StartULID is the exclusive lower page boundary. Empty denotes the
	// beginning of the changelog.
	StartULID string
	// EndULID is the inclusive upper page boundary: the ULID of the last
	// change in the page. It is also the new persisted progress.
	EndULID string

	ChangeCount int
	// Checksum is [ChangePageChecksum] recomputed over the changes in the
	// (StartULID, EndULID] range.
	Checksum string

	ConfirmedAt time.Time
}

// ChangeConsumerState is the durable state of a named consumer, returned by
// acquisition and by progress queries.
type ChangeConsumerState struct {
	Store      string
	Consumer   string
	ObjectType string

	// Lease fields. Holder is empty after an explicit release.
	Generation uint64
	Holder     string
	AcquiredAt time.Time
	ExpiresAt  time.Time

	// ProgressULID is the ULID of the last fully delivered change. A
	// continuation read starts strictly after it.
	ProgressULID string
	// LastReceipt is the most recent complete-page receipt, or nil before the
	// first confirmation.
	LastReceipt *PageReceipt

	UpdatedAt time.Time
}

// DeliveredChangeEntry is one changelog record handed to the lease holder.
type DeliveredChangeEntry struct {
	ULID   string
	Change *openfgav1.TupleChange
}

// DeliveredChangePage is the result of a lease-holder read. The worker must
// process every entry and then confirm the page using StartULID/EndULID/
// ChangeCount/Checksum to advance the persisted progress.
//
// An empty page (ChangeCount == 0) carries EndULID == StartULID and cannot be
// confirmed: it simply means the consumer has caught up to the horizon.
type DeliveredChangePage struct {
	Store      string
	Consumer   string
	ObjectType string
	Generation uint64

	StartULID string
	EndULID   string

	Entries     []DeliveredChangeEntry
	ChangeCount int
	Checksum    string

	// ExpiresAt is the lease deadline observed while serving the page, useful
	// for scheduling renewals.
	ExpiresAt time.Time
}

// Changes returns the ordered tuple changes of the page.
func (p *DeliveredChangePage) Changes() []*openfgav1.TupleChange {
	changes := make([]*openfgav1.TupleChange, 0, len(p.Entries))
	for _, e := range p.Entries {
		changes = append(changes, e.Change)
	}
	return changes
}

// AcquireChangeLeaseOptions are the parameters of
// [ChangeConsumerBackend.AcquireChangeLease].
type AcquireChangeLeaseOptions struct {
	Store    string
	Consumer string
	// Holder identifies the acquiring worker.
	Holder string
	// ObjectType pins the subscription's object-type filter on first
	// acquisition. Empty means the whole store. Subsequent acquires must
	// match the pinned scope.
	ObjectType string
	// TTL is the lease duration; the same-holder may renew before expiry
	// without switching generation.
	TTL time.Duration
	// Now is the logical acquisition time, injected for deterministic tests.
	Now time.Time
}

// ReadChangePageOptions are the parameters of
// [ChangeConsumerBackend.ReadChangePage].
type ReadChangePageOptions struct {
	Store         string
	Consumer      string
	Holder        string
	Generation    uint64
	PageSize      int
	HorizonOffset time.Duration
	Now           time.Time
}

// ConfirmChangePageOptions are the parameters of
// [ChangeConsumerBackend.ConfirmChangePage].
type ConfirmChangePageOptions struct {
	Store      string
	Consumer   string
	Holder     string
	Generation uint64

	// StartULID/EndULID/ChangeCount/Checksum must be the values returned on
	// the [DeliveredChangePage] being confirmed.
	StartULID   string
	EndULID     string
	ChangeCount int
	Checksum    string

	Now time.Time
}

// ReleaseChangeLeaseOptions are the parameters of
// [ChangeConsumerBackend.ReleaseChangeLease].
type ReleaseChangeLeaseOptions struct {
	Store      string
	Consumer   string
	Holder     string
	Generation uint64
	Now        time.Time
}

// ChangeConsumerBackend manages named, lease-protected consumers over the
// changelog.
type ChangeConsumerBackend interface {
	// AcquireChangeLease grants an exclusive lease for a named consumer:
	//
	//   - when the consumer does not exist, it is created at generation 1
	//     with the supplied object-type scope;
	//   - when the caller already holds an unexpired lease, it is renewed
	//     (TTL extended) without changing the generation;
	//   - when the existing lease has expired, the caller takes over: the
	//     generation is incremented exactly once, durable progress and
	//     receipts are retained;
	//   - when another holder's lease is still valid, ErrChangeLeaseHeld is
	//     returned.
	AcquireChangeLease(ctx context.Context, opts AcquireChangeLeaseOptions) (*ChangeConsumerState, error)

	// ReadChangePage atomically validates the lease and returns the next page
	// of changes strictly after the consumer's persisted progress, applying
	// the consumer's pinned object-type scope and the horizon offset. The
	// read snapshot and the progress lookup share one transaction boundary,
	// so the returned StartULID is exactly the durable continuation point.
	ReadChangePage(ctx context.Context, opts ReadChangePageOptions) (*DeliveredChangePage, error)

	// ConfirmChangePage atomically (a) fences the caller against the current
	// lease generation, (b) verifies that (StartULID, EndULID] still matches
	// the changelog (contiguity, count and checksum), and (c) appends the
	// complete-page receipt and advances progress to EndULID. Steps (a)-(c)
	// commit together; a failure leaves progress and receipts untouched.
	//
	// Confirming the identical (EndULID, Checksum) of the current tip is
	// idempotent and returns the existing receipt.
	ConfirmChangePage(ctx context.Context, opts ConfirmChangePageOptions) (*PageReceipt, error)

	// GetChangeConsumerState returns the durable state, including the last
	// complete-page receipt. It answers "how far has this consumer been fully
	// delivered" without requiring the lease.
	GetChangeConsumerState(ctx context.Context, store, consumer string) (*ChangeConsumerState, error)

	// GetChangePageReceipt returns the receipt at the given 1-based position
	// in the chain. It returns ErrNotFound when no such receipt exists.
	GetChangePageReceipt(ctx context.Context, store, consumer string, pageNumber uint64) (*PageReceipt, error)

	// ReleaseChangeLease voluntarily drops the lease. Only the current
	// holder presenting the current generation may release it. The next
	// acquire performs a generation switch.
	ReleaseChangeLease(ctx context.Context, opts ReleaseChangeLeaseOptions) error
}

// ChangePageChecksum computes a stable fingerprint over an ordered page of
// tuple changes. Only durable change contents (operation, tuple key and
// timestamp) are hashed, so the checksum can be recomputed by any reader
// that can replay the same range — including legacy
// [ChangelogBackend.ReadChanges] pagination.
func ChangePageChecksum(changes []*openfgav1.TupleChange) string {
	sum := sha256.New()
	for _, change := range changes {
		key := change.GetTupleKey()
		sum.Write([]byte(strconv.Itoa(int(change.GetOperation()))))
		sum.Write([]byte("|"))
		sum.Write([]byte(key.GetObject()))
		sum.Write([]byte("#"))
		sum.Write([]byte(key.GetRelation()))
		sum.Write([]byte("@"))
		sum.Write([]byte(key.GetUser()))
		sum.Write([]byte("|"))
		sum.Write([]byte(strconv.FormatInt(change.GetTimestamp().AsTime().UTC().UnixNano(), 10)))
		sum.Write([]byte("\n"))
	}
	return hex.EncodeToString(sum.Sum(nil))
}
