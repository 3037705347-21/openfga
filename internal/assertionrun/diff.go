package assertionrun

import (
	"context"
	"sort"

	"github.com/openfga/openfga/pkg/storage/assertionrun"
)

// DiffKind classifies a single assertion when comparing two completed batches.
type DiffKind string

const (
	// DiffSame means the assertion produced the same outcome in both batches.
	DiffSame DiffKind = "SAME"
	// DiffChanged means the expectation, evaluated result or status changed.
	DiffChanged DiffKind = "CHANGED"
	// DiffOnlyInA means the assertion only exists in the first batch.
	DiffOnlyInA DiffKind = "ONLY_IN_A"
	// DiffOnlyInB means the assertion only exists in the second batch.
	DiffOnlyInB DiffKind = "ONLY_IN_B"
)

// EntryDiff is the comparison of one assertion across two batches.
type EntryDiff struct {
	// Key is the stable assertion key (object#relation@user).
	Key  string   `json:"key"`
	Kind DiffKind `json:"kind"`
	// A and B are the results from the first and second batch, respectively.
	// The missing side is nil for DiffOnlyInA/DiffOnlyInB.
	A *assertionrun.Result `json:"a,omitempty"`
	B *assertionrun.Result `json:"b,omitempty"`
}

// Diff is the full comparison of two completed batches.
type Diff struct {
	BatchAID string `json:"batch_a_id"`
	BatchBID string `json:"batch_b_id"`

	Entries []*EntryDiff `json:"entries"`

	Same    int `json:"same"`
	Changed int `json:"changed"`
	OnlyInA int `json:"only_in_a"`
	OnlyInB int `json:"only_in_b"`
}

// HasChanges reports whether the two batches differ in any way (changed
// outcomes or assertions present on only one side).
func (d *Diff) HasChanges() bool {
	return d.Changed+d.OnlyInA+d.OnlyInB > 0
}

// ChangedEntries returns only the entries that differ (changed outcome or
// present on just one side), in stable key order.
func (d *Diff) ChangedEntries() []*EntryDiff {
	out := make([]*EntryDiff, 0, d.Changed+d.OnlyInA+d.OnlyInB)
	for _, e := range d.Entries {
		if e.Kind != DiffSame {
			out = append(out, e)
		}
	}
	return out
}

// CompareReports loads two completed batches and compares their assertion
// outcomes. It returns [assertionrun.ErrBatchNotCompleted] if either batch has
// not finished.
func (r *Runner) CompareReports(ctx context.Context, store, batchAID, batchBID string) (*Diff, error) {
	reportA, err := r.Report(ctx, store, batchAID)
	if err != nil {
		return nil, err
	}
	reportB, err := r.Report(ctx, store, batchBID)
	if err != nil {
		return nil, err
	}

	if reportA.Batch.Status != assertionrun.StatusCompleted {
		return nil, assertionrun.ErrBatchNotCompleted
	}
	if reportB.Batch.Status != assertionrun.StatusCompleted {
		return nil, assertionrun.ErrBatchNotCompleted
	}

	byKeyA := make(map[string]*assertionrun.Result, len(reportA.Results))
	for _, res := range reportA.Results {
		byKeyA[assertionrun.AssertionKey(res.Assertion)] = res
	}
	byKeyB := make(map[string]*assertionrun.Result, len(reportB.Results))
	for _, res := range reportB.Results {
		byKeyB[assertionrun.AssertionKey(res.Assertion)] = res
	}

	keys := make(map[string]struct{}, len(byKeyA)+len(byKeyB))
	for k := range byKeyA {
		keys[k] = struct{}{}
	}
	for k := range byKeyB {
		keys[k] = struct{}{}
	}

	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)

	diff := &Diff{BatchAID: batchAID, BatchBID: batchBID}

	for _, key := range ordered {
		a, inA := byKeyA[key]
		b, inB := byKeyB[key]

		entry := &EntryDiff{Key: key, A: a, B: b}

		switch {
		case inA && !inB:
			entry.Kind = DiffOnlyInA
			diff.OnlyInA++
		case !inA && inB:
			entry.Kind = DiffOnlyInB
			diff.OnlyInB++
		case outcomesDiffer(a, b):
			entry.Kind = DiffChanged
			diff.Changed++
		default:
			entry.Kind = DiffSame
			diff.Same++
		}

		diff.Entries = append(diff.Entries, entry)
	}

	return diff, nil
}

// outcomesDiffer reports whether the evaluated outcome of an assertion changed
// between two results. A change of expectation, status or actual result counts.
func outcomesDiffer(a, b *assertionrun.Result) bool {
	if a.Expected != b.Expected {
		return true
	}
	if a.Got != b.Got {
		return true
	}
	if a.Status != b.Status {
		return true
	}
	if a.Error != b.Error {
		return true
	}
	return false
}
