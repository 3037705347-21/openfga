// Package memory provides an in-memory implementation of the assertion run
// ledger backend. It is standalone: it does not extend or modify the main
// in-memory OpenFGA datastore, the SQL backends, or the generated mocks.
package memory

import (
	"context"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/assertionrun"
)

type batchRecord struct {
	batch      *assertionrun.Batch
	assertions []*openfgav1.Assertion
	results    map[int]*assertionrun.Result
}

// Ledger is a goroutine-safe, in-memory [assertionrun.LedgerBackend].
type Ledger struct {
	mu sync.RWMutex
	// storeID => batchID => record
	batches map[string]map[string]*batchRecord
}

// New creates a new empty in-memory assertion run ledger.
func New() *Ledger {
	return &Ledger{batches: make(map[string]map[string]*batchRecord)}
}

var _ assertionrun.LedgerBackend = (*Ledger)(nil)

// InitBatch see [assertionrun.LedgerBackend].InitBatch.
func (l *Ledger) InitBatch(_ context.Context, batch *assertionrun.Batch, assertions []*openfgav1.Assertion) (*assertionrun.Batch, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.batches[batch.StoreID]; !ok {
		l.batches[batch.StoreID] = make(map[string]*batchRecord)
	}
	if _, exists := l.batches[batch.StoreID][batch.ID]; exists {
		return nil, assertionrun.ErrBatchAlreadyExists
	}

	pinned := make([]*openfgav1.Assertion, len(assertions))
	for i, a := range assertions {
		pinned[i] = cloneAssertion(a)
	}

	stored := cloneBatch(batch)
	l.batches[stored.StoreID][stored.ID] = &batchRecord{
		batch:      stored,
		assertions: pinned,
		results:    make(map[int]*assertionrun.Result),
	}

	return cloneBatch(stored), nil
}

// UpdateBatch see [assertionrun.LedgerBackend].UpdateBatch.
func (l *Ledger) UpdateBatch(_ context.Context, store, batchID string, mutate func(*assertionrun.Batch) error) (*assertionrun.Batch, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	rec, ok := l.batches[store][batchID]
	if !ok {
		return nil, assertionrun.ErrBatchNotFound
	}

	if err := mutate(rec.batch); err != nil {
		return nil, err
	}

	return cloneBatch(rec.batch), nil
}

// GetBatch see [assertionrun.LedgerBackend].GetBatch.
func (l *Ledger) GetBatch(_ context.Context, store, batchID string) (*assertionrun.Batch, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rec, ok := l.batches[store][batchID]
	if !ok {
		return nil, assertionrun.ErrBatchNotFound
	}
	return cloneBatch(rec.batch), nil
}

// ListBatches see [assertionrun.LedgerBackend].ListBatches.
func (l *Ledger) ListBatches(_ context.Context, store string) ([]*assertionrun.Batch, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var out []*assertionrun.Batch
	for _, rec := range l.batches[store] {
		out = append(out, cloneBatch(rec.batch))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	return out, nil
}

// GetAssertions see [assertionrun.LedgerBackend].GetAssertions.
func (l *Ledger) GetAssertions(_ context.Context, store, batchID string) ([]*openfgav1.Assertion, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rec, ok := l.batches[store][batchID]
	if !ok {
		return nil, assertionrun.ErrBatchNotFound
	}

	out := make([]*openfgav1.Assertion, len(rec.assertions))
	for i, a := range rec.assertions {
		out[i] = cloneAssertion(a)
	}
	return out, nil
}

// PutResult see [assertionrun.LedgerBackend].PutResult.
func (l *Ledger) PutResult(_ context.Context, store, batchID string, result *assertionrun.Result) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	rec, ok := l.batches[store][batchID]
	if !ok {
		return assertionrun.ErrBatchNotFound
	}

	rec.results[result.Index] = cloneResult(result)
	return nil
}

// GetResults see [assertionrun.LedgerBackend].GetResults.
func (l *Ledger) GetResults(_ context.Context, store, batchID string) ([]*assertionrun.Result, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rec, ok := l.batches[store][batchID]
	if !ok {
		return nil, assertionrun.ErrBatchNotFound
	}

	indexes := make([]int, 0, len(rec.results))
	for idx := range rec.results {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)

	out := make([]*assertionrun.Result, 0, len(indexes))
	for _, idx := range indexes {
		out = append(out, cloneResult(rec.results[idx]))
	}
	return out, nil
}

func cloneBatch(in *assertionrun.Batch) *assertionrun.Batch {
	out := *in
	return &out
}

func cloneResult(in *assertionrun.Result) *assertionrun.Result {
	out := *in
	if in.Assertion != nil {
		out.Assertion = cloneAssertion(in.Assertion)
	}
	return &out
}

func cloneAssertion(in *openfgav1.Assertion) *openfgav1.Assertion {
	return proto.Clone(in).(*openfgav1.Assertion)
}
