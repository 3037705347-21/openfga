// Package storerecycle orchestrates the store recycle bin and staged,
// verified store recovery on top of a [storage.OpenFGADatastore] and a
// [storerecycle.Backend].
//
// Recycle captures a deep-copied snapshot of every model version, every
// relationship tuple and every non-empty assertion set of a store, persists
// it in the recycle backend with a retention deadline, and then hard-purges
// the live data. Recovery copies the snapshot into a staging datastore that
// is physically separate from the live one, verifies the staged content
// against the snapshot manifest, and only then atomically installs it under
// the original store id. A failed or interrupted attempt discards its
// staging datastore, so half-copied data can never be reached through the
// original store id.
package storerecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
)

// defaultRetention is how long a recycled store stays recoverable when the
// caller does not configure an explicit retention.
const defaultRetention = 7 * 24 * time.Hour

// defaultModelPageSize only affects round-trip counts while enumerating the
// model versions of a recycled store.
const defaultModelPageSize = 50

// StagingFactory creates the isolated datastore used by one recovery
// attempt. Every invocation must return a fresh, empty instance that is
// unreachable from the live datastore.
type StagingFactory func() (storage.OpenFGADatastore, error)

// Option customizes a [Service].
type Option func(*Service)

// WithRetention sets how long recycled entries remain recoverable.
func WithRetention(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.retention = d
		}
	}
}

// WithClock injects the time source (tests use a fixed clock).
func WithClock(clock func() time.Time) Option {
	return func(s *Service) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// WithModelPageSize sets the page size used to enumerate model versions.
func WithModelPageSize(n int) Option {
	return func(s *Service) {
		if n > 0 {
			s.modelPageSize = n
		}
	}
}

// WithStagingFactory overrides the staging datastore factory. It is mainly
// useful for tests that need fault injection.
func WithStagingFactory(f StagingFactory) Option {
	return func(s *Service) {
		if f != nil {
			s.newStaging = f
		}
	}
}

// WithLogger sets the service logger.
func WithLogger(l logger.Logger) Option {
	return func(s *Service) {
		if l != nil {
			s.logger = l
		}
	}
}

// WithPhaseObserver registers a callback invoked after every persisted
// progress update of a recovery. It is an observability seam for tests and
// dashboards; callbacks must return quickly and must not block the pipeline.
func WithPhaseObserver(fn func(storerecycle.Progress)) Option {
	return func(s *Service) {
		if fn != nil {
			s.observe = fn
		}
	}
}

// Service implements recycle, recovery, maintenance and query operations.
// Instances are safe for concurrent use.
type Service struct {
	live    storage.OpenFGADatastore
	backend storerecycle.Backend

	clock         func() time.Time
	retention     time.Duration
	modelPageSize int
	newStaging    StagingFactory
	logger        logger.Logger
	observe       func(storerecycle.Progress)

	// stagings holds the process-local staging datastores of non-terminal
	// sessions. Staging never survives a process restart and is dropped on
	// every terminal transition.
	stagingsMu sync.Mutex
	stagings   map[string]storage.OpenFGADatastore
}

// New builds a recovery service over the live datastore and recycle backend.
func New(live storage.OpenFGADatastore, backend storerecycle.Backend, opts ...Option) *Service {
	s := &Service{
		live:          live,
		backend:       backend,
		clock:         func() time.Time { return time.Now().UTC() },
		retention:     defaultRetention,
		modelPageSize: defaultModelPageSize,
		newStaging:    func() (storage.OpenFGADatastore, error) { return memory.New(), nil },
		logger:        logger.NewNoopLogger(),
		stagings:      make(map[string]storage.OpenFGADatastore),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Recycle captures the full content of the store into the recycle bin and
// then hard-purges it from the live datastore. Deleting a store that does
// not exist is a successful no-op, preserving the idempotent DeleteStore
// semantics. A duplicate recycle is rejected while a recovery session is
// active, even if the live store is already absent.
func (s *Service) Recycle(ctx context.Context, storeID string) error {
	// Gate before touching the live datastore: a missing store must not be
	// mistaken for "nothing to do" while a recovery is mid-flight.
	switch existing, err := s.backend.GetEntry(ctx, storeID); {
	case err == nil && existing.Status == storerecycle.EntryRecovering:
		return fmt.Errorf("recycle store %s: %w", storeID, storerecycle.ErrRecoveryActive)
	case err != nil && !errors.Is(err, storerecycle.ErrEntryNotFound):
		return fmt.Errorf("recycle store %s: inspect recycle entry: %w", storeID, err)
	}

	store, err := s.live.GetStore(ctx, storeID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("recycle store %s: inspect live store: %w", storeID, err)
	}

	now := s.clock()
	snapshot, err := s.captureSnapshot(ctx, store, now)
	if err != nil {
		return fmt.Errorf("recycle store %s: capture snapshot: %w", storeID, err)
	}

	entry := &storerecycle.Entry{
		ID:        ulid.Make().String(),
		StoreID:   storeID,
		StoreName: store.GetName(),
		Status:    storerecycle.EntryRecycled,
		DeletedAt: now,
		ExpiresAt: now.Add(s.retention),
		Manifest:  snapshot.Manifest,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.backend.PutEntry(ctx, entry, snapshot); err != nil {
		return fmt.Errorf("recycle store %s: persist recycle entry: %w", storeID, err)
	}

	// The durable recycle record exists first, so a failure here leaves a
	// recoverable (if hidden) entry rather than data loss without a record.
	if purger, ok := s.live.(storerecycle.StorePurger); ok {
		if err := purger.PurgeStore(ctx, storeID); err != nil {
			return fmt.Errorf("recycle store %s: purge live store: %w", storeID, err)
		}
	} else {
		s.logger.Warn("datastore does not implement StorePurger; falling back to legacy DeleteStore",
			zap.String("store_id", storeID))
		if err := s.live.DeleteStore(ctx, storeID); err != nil {
			return fmt.Errorf("recycle store %s: legacy delete: %w", storeID, err)
		}
	}

	s.logger.Info("store moved to recycle bin",
		zap.String("store_id", storeID),
		zap.Time("expires_at", entry.ExpiresAt),
		zap.Int("models", snapshot.Manifest.ModelsCount),
		zap.Int("tuples", snapshot.Manifest.TuplesCount))
	return nil
}

// captureSnapshot enumerates every model version, tuple and non-empty
// assertion set of the store and returns a manifest-tagged deep copy.
func (s *Service) captureSnapshot(ctx context.Context, store *openfgav1.Store, now time.Time) (*storerecycle.Snapshot, error) {
	return enumerateSnapshot(ctx, s.live, store, now, s.modelPageSize)
}

// enumerateSnapshot reads the full content visible under the given datastore
// and store header, tags it with a manifest and returns a deep copy. The same
// routine is used against the live datastore at recycle time and against the
// staging datastore at verification time.
func enumerateSnapshot(
	ctx context.Context,
	ds storage.OpenFGADatastore,
	store *openfgav1.Store,
	now time.Time,
	modelPageSize int,
) (*storerecycle.Snapshot, error) {
	storeID := store.GetId()

	models, err := readAllModels(ctx, ds, storeID, modelPageSize)
	if err != nil {
		return nil, err
	}

	tuples, err := readAllTuples(ctx, ds, storeID)
	if err != nil {
		return nil, err
	}

	assertions, err := readAllAssertions(ctx, ds, storeID, models)
	if err != nil {
		return nil, err
	}

	snapshot := &storerecycle.Snapshot{
		Store:      store,
		Models:     models,
		Tuples:     tuples,
		Assertions: assertions,
	}
	snapshot.Manifest = storerecycle.BuildManifest(snapshot, now)

	// Reads may share protobuf state with the backing datastore (the in-memory
	// assertions backend returns its stored slice directly); callers must own
	// fully independent copies.
	return storerecycle.CloneSnapshot(snapshot), nil
}

func readAllModels(ctx context.Context, ds storage.OpenFGADatastore, storeID string, pageSize int) ([]*openfgav1.AuthorizationModel, error) {
	var out []*openfgav1.AuthorizationModel
	from := ""
	for {
		page, cont, err := ds.ReadAuthorizationModels(ctx, storeID, storage.ReadAuthorizationModelsOptions{
			Pagination: storage.NewPaginationOptions(int32(pageSize), from),
		})
		if err != nil {
			return nil, fmt.Errorf("read authorization models: %w", err)
		}
		out = append(out, page...)
		if cont == "" {
			return out, nil
		}
		from = cont
	}
}

func readAllTuples(ctx context.Context, ds storage.OpenFGADatastore, storeID string) ([]*openfgav1.TupleKey, error) {
	iter, err := ds.Read(ctx, storeID, storage.ReadFilter{}, storage.ReadOptions{})
	if err != nil {
		return nil, fmt.Errorf("read tuples: %w", err)
	}
	defer iter.Stop()

	var out []*openfgav1.TupleKey
	for {
		tuple, err := iter.Next(ctx)
		if errors.Is(err, storage.ErrIteratorDone) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("iterate tuples: %w", err)
		}
		out = append(out, tuple.GetKey())
	}
}

func readAllAssertions(
	ctx context.Context,
	ds storage.OpenFGADatastore,
	storeID string,
	models []*openfgav1.AuthorizationModel,
) (map[string][]*openfgav1.Assertion, error) {
	out := make(map[string][]*openfgav1.Assertion, len(models))
	for _, m := range models {
		asserts, err := ds.ReadAssertions(ctx, storeID, m.GetId())
		if err != nil {
			return nil, fmt.Errorf("read assertions for model %s: %w", m.GetId(), err)
		}
		if len(asserts) > 0 {
			out[m.GetId()] = asserts
		}
	}
	return out, nil
}
