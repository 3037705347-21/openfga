package memory

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

// Compile-time assertions that MemoryBackend provides the optional recycle
// capabilities.
var (
	_ storerecycle.StorePurger    = (*MemoryBackend)(nil)
	_ storerecycle.StoreInstaller = (*MemoryBackend)(nil)
)

// lockAllData acquires every data lock in one fixed global order and returns
// an unlock function. Existing MemoryBackend code paths hold at most one data
// lock at a time, so the order stores -> models -> tuples -> assertions cannot
// deadlock against them.
func (s *MemoryBackend) lockAllData() func() {
	s.mutexStores.Lock()
	s.mutexModels.Lock()
	s.mutexTuples.Lock()
	s.mutexAssertions.Lock()
	return func() {
		s.mutexAssertions.Unlock()
		s.mutexTuples.Unlock()
		s.mutexModels.Unlock()
		s.mutexStores.Unlock()
	}
}

// PurgeStore hard-removes every row keyed by the store id: the store header,
// all model versions, tuples, changelog entries and assertions. It is
// idempotent: purging a missing store is a no-op.
func (s *MemoryBackend) PurgeStore(ctx context.Context, id string) error {
	_, span := tracer.Start(ctx, "memory.PurgeStore")
	defer span.End()

	unlock := s.lockAllData()
	defer unlock()

	delete(s.stores, id)
	delete(s.authorizationModels, id)
	delete(s.tuples, id)
	delete(s.changes, id)

	prefix := id + "|"
	for key := range s.assertions {
		if strings.HasPrefix(key, prefix) {
			delete(s.assertions, key)
		}
	}
	return nil
}

// InstallStore atomically publishes a store header together with all of its
// model versions, tuples and assertions. Either the whole content becomes
// visible inside one critical section, or nothing is written and
// [storage.ErrCollision] is returned because the store id is already occupied
// by a live store.
//
// Changelog history is intentionally not installed: recovery covers models,
// tuples and assertions only, and the restored store accumulates new changes
// from the takeover point onward.
func (s *MemoryBackend) InstallStore(ctx context.Context, payload storerecycle.InstallPayload) error {
	_, span := tracer.Start(ctx, "memory.InstallStore")
	defer span.End()

	storeID := payload.Store.GetId()
	if storeID == "" {
		return fmt.Errorf("install payload is missing the store id")
	}

	unlock := s.lockAllData()
	defer unlock()

	if _, exists := s.stores[storeID]; exists {
		return storage.ErrCollision
	}

	s.stores[storeID] = proto.Clone(payload.Store).(*openfgav1.Store)

	models := make(map[string]*AuthorizationModelEntry, len(payload.Models))
	latestID := ""
	for _, m := range payload.Models {
		if m.GetId() > latestID {
			latestID = m.GetId()
		}
	}
	for _, m := range payload.Models {
		models[m.GetId()] = &AuthorizationModelEntry{
			model:  proto.Clone(m).(*openfgav1.AuthorizationModel),
			latest: m.GetId() == latestID,
		}
	}
	s.authorizationModels[storeID] = models

	now := time.Now().UTC()
	records := make([]*storage.TupleRecord, 0, len(payload.Tuples))
	for _, tk := range payload.Tuples {
		var conditionName string
		var conditionContext *structpb.Struct
		if c := tk.GetCondition(); c != nil {
			conditionName = c.GetName()
			if c.GetContext() != nil {
				conditionContext = proto.Clone(c.GetContext()).(*structpb.Struct)
			}
		}
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		records = append(records, &storage.TupleRecord{
			Store:            storeID,
			ObjectType:       objectType,
			ObjectID:         objectID,
			Relation:         tk.GetRelation(),
			User:             tk.GetUser(),
			ConditionName:    conditionName,
			ConditionContext: conditionContext,
			InsertedAt:       now,
		})
	}
	s.tuples[storeID] = records
	// No changelog rows are synthesized for restored tuples.
	s.changes[storeID] = nil

	for modelID, asserts := range payload.Assertions {
		if len(asserts) == 0 {
			continue
		}
		cloned := make([]*openfgav1.Assertion, 0, len(asserts))
		for _, a := range asserts {
			cloned = append(cloned, proto.Clone(a).(*openfgav1.Assertion))
		}
		s.assertions[storeID+"|"+modelID] = cloned
	}

	return nil
}
