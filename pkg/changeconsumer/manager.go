// Package changeconsumer provides the worker-facing service for draining the
// OpenFGA changelog with named, lease-protected consumers.
//
// A pool of audit workers shares one consumer name. At any time only the
// worker holding the generation-fenced lease may read and confirm pages.
// When the holder crashes, another worker acquires the lease after the TTL
// (a generation switch) and resumes from the persisted progress, so an
// un-acknowledged page is re-delivered at most once extra while acknowledged
// pages are never replayed. Complete-page receipts make the delivery
// position independently queryable and verifiable against the legacy
// ReadChanges pagination API.
package changeconsumer

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/oklog/ulid/v2"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
)

// Backend is the persistence surface required by the [Manager]: named
// consumer state plus the changelog used to verify receipts through the
// legacy pagination path.
type Backend interface {
	storage.ChangeConsumerBackend
	storage.ChangelogBackend
}

// Default tuning.
const (
	DefaultLeaseTTL    = 30 * time.Second
	DefaultMaxPageSize = 100
)

var (
	// ErrInvalidArgument is returned for malformed manager requests.
	ErrInvalidArgument = errors.New("invalid change consumer request")

	namePattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	objectTypePattern = regexp.MustCompile(`^$|^[A-Za-z][A-Za-z0-9_-]{0,254}$`)
)

// Manager coordinates lease acquisition, continuation reads, complete-page
// confirmation and receipt verification for named changelog consumers.
type Manager struct {
	backend       Backend
	horizonOffset time.Duration
	defaultTTL    time.Duration
	maxPageSize   int
	now           func() time.Time
	logger        logger.Logger
}

// Option customizes a [Manager].
type Option func(*Manager)

// WithHorizonOffset mirrors the ReadChanges horizon: changes younger than
// the offset are not yet visible to consumers.
func WithHorizonOffset(d time.Duration) Option {
	return func(m *Manager) { m.horizonOffset = d }
}

// WithLeaseTTL sets the lease duration granted when a request does not
// specify its own TTL.
func WithLeaseTTL(d time.Duration) Option {
	return func(m *Manager) { m.defaultTTL = d }
}

// WithMaxPageSize caps the page size accepted by [Manager.ReadPage].
func WithMaxPageSize(n int) Option {
	return func(m *Manager) { m.maxPageSize = n }
}

// WithClock injects a clock, mainly for tests.
func WithClock(clock func() time.Time) Option {
	return func(m *Manager) {
		if clock != nil {
			m.now = clock
		}
	}
}

// WithLogger sets the logger.
func WithLogger(l logger.Logger) Option {
	return func(m *Manager) {
		if l != nil {
			m.logger = l
		}
	}
}

// New creates a [Manager] over the given backend.
func New(backend Backend, opts ...Option) *Manager {
	m := &Manager{
		backend:       backend,
		horizonOffset: 0,
		defaultTTL:    DefaultLeaseTTL,
		maxPageSize:   DefaultMaxPageSize,
		now:           func() time.Time { return time.Now().UTC() },
		logger:        logger.NewNoopLogger(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// AcquireLeaseRequest creates or renews an exclusive consumer lease.
type AcquireLeaseRequest struct {
	// StoreID is the store whose changelog is consumed.
	StoreID string
	// Name is the durable, named consumer. Progress and receipts are keyed
	// by (StoreID, Name).
	Name string
	// HolderID identifies the acquiring worker.
	HolderID string
	// ObjectType optionally pins the subscription to one object type. It is
	// applied on the first acquisition and must remain stable afterwards.
	ObjectType string
	// TTL optionally overrides the manager default lease duration.
	TTL time.Duration
}

// AcquireLease grants a lease (creating the consumer at generation 1),
// renews the caller's live lease without a generation switch, or performs a
// generation switch after expiry. It returns [storage.ErrChangeLeaseHeld]
// while another holder's lease is valid.
func (m *Manager) AcquireLease(ctx context.Context, req AcquireLeaseRequest) (*storage.ChangeLease, error) {
	if err := validateAcquire(req); err != nil {
		return nil, err
	}

	ttl := req.TTL
	if ttl <= 0 {
		ttl = m.defaultTTL
	}

	state, err := m.backend.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store:      req.StoreID,
		Consumer:   req.Name,
		Holder:     req.HolderID,
		ObjectType: req.ObjectType,
		TTL:        ttl,
		Now:        m.now(),
	})
	if err != nil {
		return nil, fmt.Errorf("acquire change lease for consumer %q: %w", req.Name, err)
	}
	return stateToLease(state), nil
}

// RenewLease extends the caller's current lease without changing its
// generation. It is equivalent to acquiring again with the lease's own
// identity and scope.
func (m *Manager) RenewLease(ctx context.Context, lease *storage.ChangeLease) (*storage.ChangeLease, error) {
	if lease == nil {
		return nil, fmt.Errorf("%w: lease is required", ErrInvalidArgument)
	}
	return m.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID:    lease.Store,
		Name:       lease.Consumer,
		HolderID:   lease.Holder,
		ObjectType: lease.ObjectType,
	})
}

// ReadPage returns the next page strictly after the consumer's persisted
// progress. Only the current lease holder may read. The returned page's
// StartULID is the exact durable continuation point; after processing every
// entry the caller must invoke [Manager.ConfirmPage].
func (m *Manager) ReadPage(
	ctx context.Context,
	lease *storage.ChangeLease,
	pageSize int,
) (*storage.DeliveredChangePage, error) {
	if lease == nil {
		return nil, fmt.Errorf("%w: lease is required", ErrInvalidArgument)
	}
	if pageSize < 1 || pageSize > m.maxPageSize {
		return nil, fmt.Errorf("%w: page size must be within [1, %d]", ErrInvalidArgument, m.maxPageSize)
	}

	page, err := m.backend.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store:         lease.Store,
		Consumer:      lease.Consumer,
		Holder:        lease.Holder,
		Generation:    lease.Generation,
		PageSize:      pageSize,
		HorizonOffset: m.horizonOffset,
		Now:           m.now(),
	})
	if err != nil {
		return nil, fmt.Errorf("read change page for consumer %q: %w", lease.Consumer, err)
	}
	return page, nil
}

// ConfirmPage atomically verifies the delivered page against the changelog,
// records the complete-page receipt and advances durable progress to the
// page's EndULID. Only the current lease holder may confirm. Confirming the
// page at the persisted tip is idempotent.
//
// A non-nil result error leaves progress untouched; the caller should
// re-read from the persisted progress instead of retrying the same
// confirmation blindly.
func (m *Manager) ConfirmPage(
	ctx context.Context,
	lease *storage.ChangeLease,
	page *storage.DeliveredChangePage,
) (*storage.PageReceipt, error) {
	if lease == nil || page == nil {
		return nil, fmt.Errorf("%w: lease and page are required", ErrInvalidArgument)
	}

	receipt, err := m.backend.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
		Store:       lease.Store,
		Consumer:    lease.Consumer,
		Holder:      lease.Holder,
		Generation:  lease.Generation,
		StartULID:   page.StartULID,
		EndULID:     page.EndULID,
		ChangeCount: page.ChangeCount,
		Checksum:    page.Checksum,
		Now:         m.now(),
	})
	if err != nil {
		return nil, fmt.Errorf("confirm change page for consumer %q: %w", lease.Consumer, err)
	}
	return receipt, nil
}

// Release voluntarily drops the lease so another worker can take over
// immediately (with a generation switch).
func (m *Manager) Release(ctx context.Context, lease *storage.ChangeLease) error {
	if lease == nil {
		return fmt.Errorf("%w: lease is required", ErrInvalidArgument)
	}
	if err := m.backend.ReleaseChangeLease(ctx, storage.ReleaseChangeLeaseOptions{
		Store:      lease.Store,
		Consumer:   lease.Consumer,
		Holder:     lease.Holder,
		Generation: lease.Generation,
		Now:        m.now(),
	}); err != nil {
		return fmt.Errorf("release change lease for consumer %q: %w", lease.Consumer, err)
	}
	return nil
}

// GetState answers "how far has this named consumer been fully delivered",
// returning durable progress and the last complete-page receipt. It does not
// require the lease, so any auditor can query it.
func (m *Manager) GetState(ctx context.Context, storeID, name string) (*storage.ChangeConsumerState, error) {
	if !namePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: consumer name", ErrInvalidArgument)
	}
	state, err := m.backend.GetChangeConsumerState(ctx, storeID, name)
	if err != nil {
		return nil, fmt.Errorf("get state for consumer %q: %w", name, err)
	}
	return state, nil
}

// GetReceipt returns a specific complete-page receipt by its 1-based
// position in the delivery chain.
func (m *Manager) GetReceipt(ctx context.Context, storeID, name string, pageNumber uint64) (*storage.PageReceipt, error) {
	if !namePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: consumer name", ErrInvalidArgument)
	}
	if pageNumber == 0 {
		return nil, fmt.Errorf("%w: page number must be >= 1", ErrInvalidArgument)
	}
	receipt, err := m.backend.GetChangePageReceipt(ctx, storeID, name, pageNumber)
	if err != nil {
		return nil, fmt.Errorf("get receipt %d for consumer %q: %w", pageNumber, name, err)
	}
	return receipt, nil
}

// VerifyReceipt fetches the persisted receipt at pageNumber and replays its
// (StartULID, EndULID] range through the legacy ChangelogBackend.ReadChanges
// pagination interface, recomputing the fingerprint. A nil error means the
// receipt, the durable progress and the legacy pagination agree.
func (m *Manager) VerifyReceipt(ctx context.Context, storeID, name string, pageNumber uint64) (*storage.PageReceipt, error) {
	receipt, err := m.GetReceipt(ctx, storeID, name, pageNumber)
	if err != nil {
		return nil, err
	}

	// Page size 1 makes every ReadChanges continuation token an exact change
	// ULID, so the walk stops precisely at receipt.EndULID.
	var replayed []*openfgav1.TupleChange
	token := receipt.StartULID
	end, err := ulid.Parse(receipt.EndULID)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed receipt end boundary: %v", storage.ErrInvalidPageReceipt, err)
	}
	for {
		changes, cont, err := m.backend.ReadChanges(ctx, storeID,
			storage.ReadChangesFilter{ObjectType: receipt.ObjectType},
			storage.ReadChangesOptions{Pagination: storage.NewPaginationOptions(1, token)})
		if errors.Is(err, storage.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("replay receipt %d for consumer %q: %w", pageNumber, name, err)
		}
		if len(changes) != 1 || cont == "" {
			return nil, fmt.Errorf("%w: legacy pagination returned an unexpected page", storage.ErrInvalidPageReceipt)
		}
		replayed = append(replayed, changes[0])
		token = cont
		if cont == receipt.EndULID {
			break
		}
		cur, err := ulid.Parse(cont)
		if err != nil {
			return nil, fmt.Errorf("%w: malformed legacy continuation token", storage.ErrInvalidPageReceipt)
		}
		if cur.Compare(end) > 0 {
			return nil, fmt.Errorf("%w: legacy pagination overran receipt boundary %s", storage.ErrInvalidPageReceipt, receipt.EndULID)
		}
	}

	if len(replayed) != receipt.ChangeCount {
		return nil, fmt.Errorf("%w: receipt claims %d changes, legacy replay found %d",
			storage.ErrPageChecksumMismatch, receipt.ChangeCount, len(replayed))
	}
	if got := storage.ChangePageChecksum(replayed); got != receipt.Checksum {
		return nil, fmt.Errorf("%w: checksum %s does not reproduce from legacy pagination",
			storage.ErrPageChecksumMismatch, receipt.EndULID)
	}
	return receipt, nil
}

func validateAcquire(req AcquireLeaseRequest) error {
	if req.StoreID == "" {
		return fmt.Errorf("%w: store id is required", ErrInvalidArgument)
	}
	if !namePattern.MatchString(req.Name) {
		return fmt.Errorf("%w: consumer name must match %s", ErrInvalidArgument, namePattern.String())
	}
	if !namePattern.MatchString(req.HolderID) {
		return fmt.Errorf("%w: holder id must match %s", ErrInvalidArgument, namePattern.String())
	}
	if !objectTypePattern.MatchString(req.ObjectType) {
		return fmt.Errorf("%w: object type filter", ErrInvalidArgument)
	}
	if req.TTL < 0 {
		return fmt.Errorf("%w: lease ttl must be positive", ErrInvalidArgument)
	}
	return nil
}

func stateToLease(state *storage.ChangeConsumerState) *storage.ChangeLease {
	return &storage.ChangeLease{
		Store:      state.Store,
		Consumer:   state.Consumer,
		Holder:     state.Holder,
		Generation: state.Generation,
		ObjectType: state.ObjectType,
		AcquiredAt: state.AcquiredAt,
		ExpiresAt:  state.ExpiresAt,
	}
}
