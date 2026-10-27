package memory

import (
	"context"
	"time"

	"github.com/oklog/ulid/v2"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

// Compile-time assertion that MemoryBackend supports named change consumers.
var _ storage.ChangeConsumerBackend = (*MemoryBackend)(nil)

// memChangeConsumerState is the durable state of a named change consumer.
// GUARDED_BY(MemoryBackend.mutexChangeConsumers).
type memChangeConsumerState struct {
	objectType string
	generation uint64
	holder     string
	acquiredAt time.Time
	expiresAt  time.Time

	// progressULID is the ULID of the last fully confirmed change.
	progressULID string
	// receipts is the append-only, gap-free page receipt chain.
	receipts []*storage.PageReceipt

	updatedAt time.Time
}

func consumerNow(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now.UTC()
}

// AcquireChangeLease see [storage.ChangeConsumerBackend].
func (s *MemoryBackend) AcquireChangeLease(ctx context.Context, opts storage.AcquireChangeLeaseOptions) (*storage.ChangeConsumerState, error) {
	_, span := tracer.Start(ctx, "memory.AcquireChangeLease")
	defer span.End()

	now := consumerNow(opts.Now)

	s.mutexChangeConsumers.Lock()
	defer s.mutexChangeConsumers.Unlock()

	consumers := s.changeConsumers[opts.Store]
	state, ok := consumers[opts.Consumer]
	if !ok {
		state = &memChangeConsumerState{
			objectType: opts.ObjectType,
			generation: 1,
			holder:     opts.Holder,
			acquiredAt: now,
			expiresAt:  now.Add(opts.TTL),
			updatedAt:  now,
		}
		if consumers == nil {
			consumers = make(map[string]*memChangeConsumerState)
			s.changeConsumers[opts.Store] = consumers
		}
		consumers[opts.Consumer] = state
		return snapshotChangeConsumerState(opts.Store, opts.Consumer, state), nil
	}

	if state.objectType != opts.ObjectType {
		return nil, storage.ErrChangeConsumerScopeMismatch
	}

	switch {
	case state.holder == opts.Holder && now.Before(state.expiresAt):
		// Renewal by the live holder: generation is stable.
		state.expiresAt = now.Add(opts.TTL)
	case state.holder == "" || !now.Before(state.expiresAt):
		// Released or expired: takeover always performs a generation switch.
		state.generation++
		state.holder = opts.Holder
		state.acquiredAt = now
		state.expiresAt = now.Add(opts.TTL)
	default:
		return nil, storage.ErrChangeLeaseHeld
	}

	state.updatedAt = now
	return snapshotChangeConsumerState(opts.Store, opts.Consumer, state), nil
}

// ReadChangePage see [storage.ChangeConsumerBackend].
func (s *MemoryBackend) ReadChangePage(ctx context.Context, opts storage.ReadChangePageOptions) (*storage.DeliveredChangePage, error) {
	_, span := tracer.Start(ctx, "memory.ReadChangePage")
	defer span.End()

	now := consumerNow(opts.Now)

	// Fixed lock order: mutexTuples first, then mutexChangeConsumers. The
	// progress lookup and the changelog read therefore share one snapshot:
	// no Write can append a change between "where do I continue" and "give me
	// the next page".
	s.mutexTuples.RLock()
	defer s.mutexTuples.RUnlock()
	s.mutexChangeConsumers.RLock()
	defer s.mutexChangeConsumers.RUnlock()

	state, err := s.liveLeaseLocked(opts.Store, opts.Consumer, opts.Holder, opts.Generation, now)
	if err != nil {
		return nil, err
	}

	var after *ulid.ULID
	if state.progressULID != "" {
		parsed, err := ulid.Parse(state.progressULID)
		if err != nil {
			return nil, storage.ErrInvalidContinuationToken
		}
		after = &parsed
	}

	cutoff := now.Add(-opts.HorizonOffset)
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = storage.DefaultPageSize
	}

	page := &storage.DeliveredChangePage{
		Store:      opts.Store,
		Consumer:   opts.Consumer,
		ObjectType: state.objectType,
		Generation: state.generation,
		StartULID:  state.progressULID,
		ExpiresAt:  state.expiresAt,
	}

	for _, rec := range s.changes[opts.Store] {
		if page.ChangeCount >= pageSize {
			break
		}
		if state.objectType != "" &&
			!hasObjectTypePrefix(rec.Change.GetTupleKey().GetObject(), state.objectType) {
			continue
		}
		if opts.HorizonOffset > 0 && rec.Change.GetTimestamp().AsTime().After(cutoff) {
			// Changelog is ULID/ time ordered ascending; everything later is
			// also beyond the horizon.
			break
		}
		if after != nil && rec.Ulid.Compare(*after) <= 0 {
			continue
		}
		page.Entries = append(page.Entries, storage.DeliveredChangeEntry{
			ULID:   rec.Ulid.String(),
			Change: rec.Change,
		})
		page.ChangeCount++
		page.EndULID = rec.Ulid.String()
	}

	// An empty page is reported at the current boundary so callers can
	// persistently detect "caught up" and resume later.
	if page.ChangeCount == 0 {
		page.EndULID = page.StartULID
	}
	page.Checksum = storage.ChangePageChecksum(page.Changes())
	return page, nil
}

// ConfirmChangePage see [storage.ChangeConsumerBackend].
func (s *MemoryBackend) ConfirmChangePage(ctx context.Context, opts storage.ConfirmChangePageOptions) (*storage.PageReceipt, error) {
	_, span := tracer.Start(ctx, "memory.ConfirmChangePage")
	defer span.End()

	now := consumerNow(opts.Now)

	s.mutexTuples.RLock()
	defer s.mutexTuples.RUnlock()
	s.mutexChangeConsumers.Lock()
	defer s.mutexChangeConsumers.Unlock()

	state, err := s.liveLeaseLocked(opts.Store, opts.Consumer, opts.Holder, opts.Generation, now)
	if err != nil {
		return nil, err
	}

	// Validate the claimed boundary.
	if opts.EndULID == "" || opts.EndULID == opts.StartULID {
		return nil, storage.ErrInvalidPageReceipt
	}

	end, err := ulid.Parse(opts.EndULID)
	if err != nil {
		return nil, storage.ErrInvalidPageReceipt
	}
	var start ulid.ULID
	if opts.StartULID != "" {
		start, err = ulid.Parse(opts.StartULID)
		if err != nil {
			return nil, storage.ErrInvalidPageReceipt
		}
		if end.Compare(start) <= 0 {
			return nil, storage.ErrInvalidPageReceipt
		}
	}

	// Handle the persisted tip before chaining: a tip is never rewritten, a
	// confirmation below it is a regression, and an exact repeat is
	// idempotent (this also lets concurrent confirmations converge).
	if n := len(state.receipts); n > 0 {
		tip := state.receipts[n-1]
		tipEnd, err := ulid.Parse(tip.EndULID)
		if err != nil {
			return nil, storage.ErrInvalidPageReceipt
		}
		switch end.Compare(tipEnd) {
		case -1:
			return nil, storage.ErrInvalidPageReceipt
		case 0:
			if tip.StartULID != opts.StartULID ||
				tip.ChangeCount != opts.ChangeCount ||
				tip.Checksum != opts.Checksum {
				return nil, storage.ErrInvalidPageReceipt
			}
			return clonePageReceipt(tip), nil
		}
	}

	// New receipts must chain directly from durable progress.
	if opts.StartULID != state.progressULID {
		return nil, storage.ErrInvalidPageReceipt
	}

	// Replay the sealed range against the changelog in the same transaction.
	// Records inside (start, end] were all older than the read horizon when
	// the page was delivered, so no horizon is applied here.
	changes := make([]*openfgav1.TupleChange, 0)
	last := start
	for _, rec := range s.changes[opts.Store] {
		if rec.Ulid.Compare(start) <= 0 {
			continue
		}
		if rec.Ulid.Compare(end) > 0 {
			break
		}
		if state.objectType != "" &&
			!hasObjectTypePrefix(rec.Change.GetTupleKey().GetObject(), state.objectType) {
			continue
		}
		changes = append(changes, rec.Change)
		last = rec.Ulid
	}

	if len(changes) == 0 || last != end {
		// The claimed end boundary does not exist in the changelog, or the
		// range is empty: the worker is confirming something never delivered.
		return nil, storage.ErrInvalidPageReceipt
	}
	if len(changes) != opts.ChangeCount || storage.ChangePageChecksum(changes) != opts.Checksum {
		return nil, storage.ErrPageChecksumMismatch
	}

	receipt := &storage.PageReceipt{
		Store:       opts.Store,
		Consumer:    opts.Consumer,
		ObjectType:  state.objectType,
		PageNumber:  uint64(len(state.receipts) + 1),
		Generation:  state.generation,
		StartULID:   opts.StartULID,
		EndULID:     opts.EndULID,
		ChangeCount: opts.ChangeCount,
		Checksum:    opts.Checksum,
		ConfirmedAt: now,
	}
	state.receipts = append(state.receipts, receipt)
	state.progressULID = opts.EndULID
	state.updatedAt = now

	return clonePageReceipt(receipt), nil
}

// GetChangeConsumerState see [storage.ChangeConsumerBackend].
func (s *MemoryBackend) GetChangeConsumerState(ctx context.Context, store, consumer string) (*storage.ChangeConsumerState, error) {
	_, span := tracer.Start(ctx, "memory.GetChangeConsumerState")
	defer span.End()

	s.mutexChangeConsumers.RLock()
	defer s.mutexChangeConsumers.RUnlock()

	state, err := s.consumerStateLocked(store, consumer)
	if err != nil {
		return nil, err
	}
	return snapshotChangeConsumerState(store, consumer, state), nil
}

// GetChangePageReceipt see [storage.ChangeConsumerBackend].
func (s *MemoryBackend) GetChangePageReceipt(ctx context.Context, store, consumer string, pageNumber uint64) (*storage.PageReceipt, error) {
	_, span := tracer.Start(ctx, "memory.GetChangePageReceipt")
	defer span.End()

	s.mutexChangeConsumers.RLock()
	defer s.mutexChangeConsumers.RUnlock()

	state, err := s.consumerStateLocked(store, consumer)
	if err != nil {
		return nil, err
	}
	if pageNumber == 0 || int(pageNumber) > len(state.receipts) {
		return nil, storage.ErrNotFound
	}
	return clonePageReceipt(state.receipts[pageNumber-1]), nil
}

// ReleaseChangeLease see [storage.ChangeConsumerBackend].
func (s *MemoryBackend) ReleaseChangeLease(ctx context.Context, opts storage.ReleaseChangeLeaseOptions) error {
	_, span := tracer.Start(ctx, "memory.ReleaseChangeLease")
	defer span.End()

	now := consumerNow(opts.Now)

	s.mutexChangeConsumers.Lock()
	defer s.mutexChangeConsumers.Unlock()

	state, err := s.consumerStateLocked(opts.Store, opts.Consumer)
	if err != nil {
		return err
	}
	if !state.leaseValid(opts.Holder, opts.Generation, now) {
		return storage.ErrChangeLeaseFenced
	}

	state.holder = ""
	state.acquiredAt = time.Time{}
	state.expiresAt = time.Time{}
	state.updatedAt = now
	return nil
}

func (s *MemoryBackend) consumerStateLocked(store, consumer string) (*memChangeConsumerState, error) {
	state, ok := s.changeConsumers[store][consumer]
	if !ok {
		return nil, storage.ErrChangeConsumerNotFound
	}
	return state, nil
}

// liveLeaseLocked returns the consumer state only when holder and generation
// match and the lease has not expired.
func (s *MemoryBackend) liveLeaseLocked(store, consumer, holder string, generation uint64, now time.Time) (*memChangeConsumerState, error) {
	state, err := s.consumerStateLocked(store, consumer)
	if err != nil {
		return nil, err
	}
	if !state.leaseValid(holder, generation, now) {
		return nil, storage.ErrChangeLeaseFenced
	}
	return state, nil
}

func (st *memChangeConsumerState) leaseValid(holder string, generation uint64, now time.Time) bool {
	return st.holder == holder && st.generation == generation && now.Before(st.expiresAt)
}

func hasObjectTypePrefix(object, objectType string) bool {
	if len(object) <= len(objectType) {
		return false
	}
	return object[:len(objectType)] == objectType && object[len(objectType)] == ':'
}

func snapshotChangeConsumerState(store, consumer string, st *memChangeConsumerState) *storage.ChangeConsumerState {
	out := &storage.ChangeConsumerState{
		Store:        store,
		Consumer:     consumer,
		ObjectType:   st.objectType,
		Generation:   st.generation,
		Holder:       st.holder,
		AcquiredAt:   st.acquiredAt,
		ExpiresAt:    st.expiresAt,
		ProgressULID: st.progressULID,
		UpdatedAt:    st.updatedAt,
	}
	if n := len(st.receipts); n > 0 {
		out.LastReceipt = clonePageReceipt(st.receipts[n-1])
	}
	return out
}

func clonePageReceipt(r *storage.PageReceipt) *storage.PageReceipt {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}
