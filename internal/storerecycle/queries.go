package storerecycle

import (
	"context"
	"fmt"

	"github.com/openfga/openfga/pkg/storage/storerecycle"
)

// GetRecovery returns one recovery session of a store.
func (s *Service) GetRecovery(ctx context.Context, storeID, sessionID string) (*storerecycle.RecoverySession, error) {
	sess, err := s.backend.GetRecovery(ctx, storeID, sessionID)
	if err != nil {
		return nil, fmt.Errorf("get recovery %s for store %s: %w", sessionID, storeID, err)
	}
	return sess, nil
}

// ListRecoveries returns every recovery session of a store, newest first.
func (s *Service) ListRecoveries(ctx context.Context, storeID string) ([]*storerecycle.RecoverySession, error) {
	sessions, err := s.backend.ListRecoveries(ctx, storeID)
	if err != nil {
		return nil, fmt.Errorf("list recoveries for store %s: %w", storeID, err)
	}
	return sessions, nil
}

// ListTakeovers returns the takeover audit records of a store, oldest first.
func (s *Service) ListTakeovers(ctx context.Context, storeID string) ([]*storerecycle.TakeoverRecord, error) {
	records, err := s.backend.ListTakeovers(ctx, storeID)
	if err != nil {
		return nil, fmt.Errorf("list takeovers for store %s: %w", storeID, err)
	}
	return records, nil
}

// ListRecycleBin returns every known recycle entry header.
func (s *Service) ListRecycleBin(ctx context.Context) ([]*storerecycle.Entry, error) {
	entries, err := s.backend.ListEntries(ctx)
	if err != nil {
		return nil, fmt.Errorf("list recycle bin: %w", err)
	}
	return entries, nil
}

// GetRecycleEntry returns the current recycle entry header for a store.
func (s *Service) GetRecycleEntry(ctx context.Context, storeID string) (*storerecycle.Entry, error) {
	entry, err := s.backend.GetEntry(ctx, storeID)
	if err != nil {
		return nil, fmt.Errorf("get recycle entry for store %s: %w", storeID, err)
	}
	return entry, nil
}
