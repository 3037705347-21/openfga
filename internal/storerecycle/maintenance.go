package storerecycle

import (
	"context"
	"fmt"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
)

// staleRecoveryReason is recorded on sessions discarded by the startup sweep.
const staleRecoveryReason = "recovery interrupted by restart"

// SweepStale reconciles non-terminal recovery sessions, typically at process
// startup. Staging datastores are process-local: a session found without a
// live staging handle (every session after a restart) cannot be trusted, so
// it is moved to DISCARDED and its entry returned to RECYCLED, allowing a
// fresh recovery afterwards. In-process staging handles that are still
// registered (e.g. a request canceled mid-flight) are closed and dropped too.
func (s *Service) SweepStale(ctx context.Context) ([]*storerecycle.RecoverySession, error) {
	// Drop every process-local staging handle first: anything non-terminal is
	// stale from this point on.
	s.stagingsMu.Lock()
	handles := s.stagings
	s.stagings = make(map[string]storage.OpenFGADatastore, len(handles))
	s.stagingsMu.Unlock()
	for _, ds := range handles {
		ds.Close()
	}

	swept, err := s.backend.DiscardStale(ctx, staleRecoveryReason, s.clock())
	if err != nil {
		return nil, fmt.Errorf("sweep stale recoveries: %w", err)
	}
	return swept, nil
}

// PurgeExpired permanently removes recycle entries whose retention has
// elapsed, subject to the backend gate: an expired entry is kept whenever a
// non-terminal recovery session exists for it. It returns the purged store
// ids.
func (s *Service) PurgeExpired(ctx context.Context) ([]string, error) {
	entries, err := s.backend.PurgeExpired(ctx, s.clock())
	if err != nil {
		return nil, fmt.Errorf("purge expired entries: %w", err)
	}
	storeIDs := make([]string, 0, len(entries))
	for _, entry := range entries {
		storeIDs = append(storeIDs, entry.StoreID)
	}
	return storeIDs, nil
}
