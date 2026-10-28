// Package memory provides a standalone in-memory implementation of
// [storerecycle.Backend]. It is safe for concurrent use and is intentionally
// independent of the SQL datastores and the generated mocks.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/openfga/openfga/pkg/storage/storerecycle"
)

// Backend is the in-memory [storerecycle.Backend].
//
// Instances may be safely shared by multiple goroutines. All state machine
// transitions happen while holding mu, so the entry/session invariants cannot
// be observed mid-transition.
type Backend struct {
	mu sync.RWMutex

	// entries holds the current generation of every store. The snapshot
	// payload is nil once the entry is RESTORED or PURGED.
	entries map[string]*entryRecord

	// sessions and takeovers are append-only histories keyed by store id.
	// Sessions are removed together with a purged entry; takeover records are
	// retained beyond purges for audit.
	sessions  map[string][]*storerecycle.RecoverySession
	takeovers map[string][]*storerecycle.TakeoverRecord
}

type entryRecord struct {
	header   *storerecycle.Entry
	snapshot *storerecycle.Snapshot
}

var _ storerecycle.Backend = (*Backend)(nil)

// New creates an empty recycle backend.
func New() *Backend {
	return &Backend{
		entries:   make(map[string]*entryRecord),
		sessions:  make(map[string][]*storerecycle.RecoverySession),
		takeovers: make(map[string][]*storerecycle.TakeoverRecord),
	}
}

// PutEntry see [storerecycle.Backend.PutEntry].
func (b *Backend) PutEntry(_ context.Context, entry *storerecycle.Entry, snapshot *storerecycle.Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if existing := b.entries[entry.StoreID]; existing != nil && existing.header.Status == storerecycle.EntryRecovering {
		return storerecycle.ErrRecoveryActive
	}

	b.entries[entry.StoreID] = &entryRecord{
		header:   cloneEntry(entry),
		snapshot: storerecycle.CloneSnapshot(snapshot),
	}
	// A new generation replaces the session history; takeover records are an
	// audit trail of the store and survive.
	delete(b.sessions, entry.StoreID)

	return nil
}

// GetEntry see [storerecycle.Backend.GetEntry].
func (b *Backend) GetEntry(_ context.Context, storeID string) (*storerecycle.Entry, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	rec := b.entries[storeID]
	if rec == nil {
		return nil, storerecycle.ErrEntryNotFound
	}
	return cloneEntry(rec.header), nil
}

// GetSnapshot see [storerecycle.Backend.GetSnapshot].
func (b *Backend) GetSnapshot(_ context.Context, storeID string) (*storerecycle.Snapshot, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	rec := b.entries[storeID]
	if rec == nil {
		return nil, storerecycle.ErrEntryNotFound
	}
	if rec.snapshot == nil {
		return nil, storerecycle.ErrSnapshotUnavailable
	}
	return storerecycle.CloneSnapshot(rec.snapshot), nil
}

// ListEntries see [storerecycle.Backend.ListEntries].
func (b *Backend) ListEntries(_ context.Context) ([]*storerecycle.Entry, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.entries))
	for id := range b.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]*storerecycle.Entry, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneEntry(b.entries[id].header))
	}
	return out, nil
}

// InitRecovery see [storerecycle.Backend.InitRecovery].
func (b *Backend) InitRecovery(_ context.Context, storeID string, session *storerecycle.RecoverySession) (*storerecycle.RecoverySession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	rec := b.entries[storeID]
	if rec == nil {
		return nil, storerecycle.ErrEntryNotFound
	}

	for _, s := range b.sessions[storeID] {
		if !s.Status.Terminal() {
			return nil, storerecycle.ErrRecoveryActive
		}
	}

	switch rec.header.Status {
	case storerecycle.EntryRecovering:
		return nil, storerecycle.ErrRecoveryActive
	case storerecycle.EntryRecycled:
	default:
		return nil, storerecycle.ErrEntryNotRecycled
	}

	stored := cloneSession(session)
	stored.StoreID = storeID
	stored.EntryID = rec.header.ID
	if stored.Status == "" {
		stored.Status = storerecycle.RecoveryRunning
	}

	rec.header.Status = storerecycle.EntryRecovering
	b.sessions[storeID] = append(b.sessions[storeID], stored)

	return cloneSession(stored), nil
}

// UpdateRecovery see [storerecycle.Backend.UpdateRecovery].
func (b *Backend) UpdateRecovery(_ context.Context, storeID, sessionID string, mutate func(*storerecycle.RecoverySession) error) (*storerecycle.RecoverySession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.findSessionLocked(storeID, sessionID)
	if s == nil {
		return nil, storerecycle.ErrRecoveryNotFound
	}
	if s.Status.Terminal() {
		return nil, storerecycle.ErrRecoveryNotActive
	}

	if err := mutate(s); err != nil {
		return nil, err
	}
	return cloneSession(s), nil
}

// CompleteRecovery see [storerecycle.Backend.CompleteRecovery].
func (b *Backend) CompleteRecovery(_ context.Context, storeID, sessionID string, at time.Time) (*storerecycle.RecoverySession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	rec := b.entries[storeID]
	if rec == nil {
		return nil, storerecycle.ErrEntryNotFound
	}
	s := b.findSessionLocked(storeID, sessionID)
	if s == nil {
		return nil, storerecycle.ErrRecoveryNotFound
	}
	if s.Status.Terminal() {
		return nil, storerecycle.ErrRecoveryNotActive
	}

	s.Status = storerecycle.RecoveryCompleted
	s.FailReason = ""
	s.DiscardReason = ""
	s.CompletedAt = at
	s.HeartbeatAt = at
	s.UpdatedAt = at
	s.Progress.Phase = storerecycle.PhaseTakeover
	// The takeover is complete: every copied/verified counter equals its total.
	fillProgressTotals(&s.Progress)

	rec.header.Status = storerecycle.EntryRestored
	rec.header.UpdatedAt = at
	rec.snapshot = nil

	return cloneSession(s), nil
}

// FailRecovery see [storerecycle.Backend.FailRecovery].
func (b *Backend) FailRecovery(_ context.Context, storeID, sessionID, reason string, at time.Time) (*storerecycle.RecoverySession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	rec := b.entries[storeID]
	if rec == nil {
		return nil, storerecycle.ErrEntryNotFound
	}
	s := b.findSessionLocked(storeID, sessionID)
	if s == nil {
		return nil, storerecycle.ErrRecoveryNotFound
	}
	if s.Status.Terminal() {
		return nil, storerecycle.ErrRecoveryNotActive
	}

	s.Status = storerecycle.RecoveryFailed
	s.FailReason = reason
	s.CompletedAt = at
	s.HeartbeatAt = at
	s.UpdatedAt = at

	rec.header.Status = storerecycle.EntryRecycled
	rec.header.UpdatedAt = at

	return cloneSession(s), nil
}

// DiscardStale see [storerecycle.Backend.DiscardStale].
func (b *Backend) DiscardStale(_ context.Context, reason string, at time.Time) ([]*storerecycle.RecoverySession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var affected []*storerecycle.RecoverySession
	for storeID, sessions := range b.sessions {
		rec := b.entries[storeID]
		for _, s := range sessions {
			if s.Status.Terminal() {
				continue
			}
			s.Status = storerecycle.RecoveryDiscarded
			s.DiscardReason = reason
			s.CompletedAt = at
			s.HeartbeatAt = at
			s.UpdatedAt = at
			affected = append(affected, cloneSession(s))

			if rec != nil && rec.header.Status == storerecycle.EntryRecovering {
				rec.header.Status = storerecycle.EntryRecycled
				rec.header.UpdatedAt = at
			}
		}
	}

	// Deterministic order for callers and tests.
	sort.Slice(affected, func(i, j int) bool {
		if affected[i].StoreID != affected[j].StoreID {
			return affected[i].StoreID < affected[j].StoreID
		}
		return affected[i].ID < affected[j].ID
	})
	return affected, nil
}

// GetRecovery see [storerecycle.Backend.GetRecovery].
func (b *Backend) GetRecovery(_ context.Context, storeID, sessionID string) (*storerecycle.RecoverySession, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	s := b.findSessionLocked(storeID, sessionID)
	if s == nil {
		return nil, storerecycle.ErrRecoveryNotFound
	}
	return cloneSession(s), nil
}

// ListRecoveries see [storerecycle.Backend.ListRecoveries].
func (b *Backend) ListRecoveries(_ context.Context, storeID string) ([]*storerecycle.RecoverySession, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sessions := b.sessions[storeID]
	out := make([]*storerecycle.RecoverySession, 0, len(sessions))
	for i := len(sessions) - 1; i >= 0; i-- {
		out = append(out, cloneSession(sessions[i]))
	}
	return out, nil
}

// AppendTakeover see [storerecycle.Backend.AppendTakeover].
func (b *Backend) AppendTakeover(_ context.Context, record *storerecycle.TakeoverRecord) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.takeovers[record.StoreID] = append(b.takeovers[record.StoreID], cloneTakeover(record))
	return nil
}

// ListTakeovers see [storerecycle.Backend.ListTakeovers].
func (b *Backend) ListTakeovers(_ context.Context, storeID string) ([]*storerecycle.TakeoverRecord, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	records := b.takeovers[storeID]
	out := make([]*storerecycle.TakeoverRecord, 0, len(records))
	for _, r := range records {
		out = append(out, cloneTakeover(r))
	}
	return out, nil
}

// PurgeExpired see [storerecycle.Backend.PurgeExpired].
func (b *Backend) PurgeExpired(_ context.Context, now time.Time) ([]*storerecycle.Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ids := make([]string, 0, len(b.entries))
	for id := range b.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var purged []*storerecycle.Entry
	for _, storeID := range ids {
		rec := b.entries[storeID]
		if rec.header.Status != storerecycle.EntryRecycled {
			continue
		}
		if !rec.header.ExpiresAt.Before(now) {
			continue
		}
		active := false
		for _, s := range b.sessions[storeID] {
			if !s.Status.Terminal() {
				active = true
				break
			}
		}
		if active {
			continue
		}

		purged = append(purged, cloneEntry(rec.header))
		delete(b.entries, storeID)
		delete(b.sessions, storeID)
		// takeovers are intentionally retained
	}
	return purged, nil
}

func (b *Backend) findSessionLocked(storeID, sessionID string) *storerecycle.RecoverySession {
	for _, s := range b.sessions[storeID] {
		if s.ID == sessionID {
			return s
		}
	}
	return nil
}

func fillProgressTotals(p *storerecycle.Progress) {
	p.ModelsCopied = p.ModelsTotal
	p.ModelsVerified = p.ModelsTotal
	p.TuplesCopied = p.TuplesTotal
	p.TuplesVerified = p.TuplesTotal
	p.AssertionModelsCopied = p.AssertionModelsTotal
	p.AssertionModelsVerified = p.AssertionModelsTotal
	p.AssertionsCopied = p.AssertionsTotal
	p.AssertionsVerified = p.AssertionsTotal
}

func cloneEntry(in *storerecycle.Entry) *storerecycle.Entry {
	out := *in
	return &out
}

func cloneSession(in *storerecycle.RecoverySession) *storerecycle.RecoverySession {
	out := *in
	return &out
}

func cloneTakeover(in *storerecycle.TakeoverRecord) *storerecycle.TakeoverRecord {
	out := *in
	return &out
}
