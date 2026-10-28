package storerecycle

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func model(id string, typeNames ...string) *openfgav1.AuthorizationModel {
	m := &openfgav1.AuthorizationModel{
		Id:            id,
		SchemaVersion: "1.1",
	}
	for _, n := range typeNames {
		m.TypeDefinitions = append(m.TypeDefinitions, &openfgav1.TypeDefinition{Type: n})
	}
	return m
}

func tuple(object, relation, user, condition string) *openfgav1.TupleKey {
	tk := &openfgav1.TupleKey{Object: object, Relation: relation, User: user}
	if condition != "" {
		tk.Condition = &openfgav1.RelationshipCondition{Name: condition}
	}
	return tk
}

func assertion(object, relation, user string, expectation bool) *openfgav1.Assertion {
	return &openfgav1.Assertion{
		TupleKey:    &openfgav1.AssertionTupleKey{Object: object, Relation: relation, User: user},
		Expectation: expectation,
	}
}

func TestFingerprintsAreOrderIndependent(t *testing.T) {
	modelsA := []*openfgav1.AuthorizationModel{model("01HZZ0", "user"), model("01HZZ1", "document")}
	modelsB := []*openfgav1.AuthorizationModel{model("01HZZ1", "document"), model("01HZZ0", "user")}

	if FingerprintModels(modelsA) != FingerprintModels(modelsB) {
		t.Fatalf("model fingerprint must be order-independent")
	}

	tuplesA := []*openfgav1.TupleKey{
		tuple("document:1", "viewer", "user:a", ""),
		tuple("document:2", "viewer", "user:b", "in_business_hours"),
	}
	tuplesB := []*openfgav1.TupleKey{
		tuple("document:2", "viewer", "user:b", "in_business_hours"),
		tuple("document:1", "viewer", "user:a", ""),
	}
	if FingerprintTuples(tuplesA) != FingerprintTuples(tuplesB) {
		t.Fatalf("tuple fingerprint must be order-independent")
	}

	assertsA := map[string][]*openfgav1.Assertion{
		"01HZZ1": {assertion("document:1", "viewer", "user:a", true)},
		"01HZZ0": {assertion("user:x", "assignee", "user:y", false)},
	}
	assertsB := map[string][]*openfgav1.Assertion{
		"01HZZ0": {assertion("user:x", "assignee", "user:y", false)},
		"01HZZ1": {assertion("document:1", "viewer", "user:a", true)},
	}
	if FingerprintAssertions(assertsA) != FingerprintAssertions(assertsB) {
		t.Fatalf("assertion fingerprint must be order-independent")
	}
}

func TestFingerprintsDetectContentChanges(t *testing.T) {
	base := []*openfgav1.TupleKey{
		tuple("document:1", "viewer", "user:a", ""),
		tuple("document:2", "viewer", "user:b", "cond"),
	}

	cases := []struct {
		name   string
		mutate func(tks []*openfgav1.TupleKey) []*openfgav1.TupleKey
	}{
		{
			name: "object changed",
			mutate: func(tks []*openfgav1.TupleKey) []*openfgav1.TupleKey {
				out := cloneTuples(tks)
				out[0].Object = "document:9"
				return out
			},
		},
		{
			name: "relation changed",
			mutate: func(tks []*openfgav1.TupleKey) []*openfgav1.TupleKey {
				out := cloneTuples(tks)
				out[0].Relation = "owner"
				return out
			},
		},
		{
			name: "user changed",
			mutate: func(tks []*openfgav1.TupleKey) []*openfgav1.TupleKey {
				out := cloneTuples(tks)
				out[0].User = "user:z"
				return out
			},
		},
		{
			name: "condition removed",
			mutate: func(tks []*openfgav1.TupleKey) []*openfgav1.TupleKey {
				out := cloneTuples(tks)
				out[1].Condition = nil
				return out
			},
		},
		{
			name: "condition context added",
			mutate: func(tks []*openfgav1.TupleKey) []*openfgav1.TupleKey {
				out := cloneTuples(tks)
				ctx, _ := structpb.NewStruct(map[string]any{"k": "v"})
				out[1].Condition.Context = ctx
				return out
			},
		},
	}

	original := FingerprintTuples(base)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FingerprintTuples(tc.mutate(base)); got == original {
				t.Fatalf("fingerprint must change when %s", tc.name)
			}
		})
	}

	if FingerprintModels([]*openfgav1.AuthorizationModel{model("01", "user")}) ==
		FingerprintModels([]*openfgav1.AuthorizationModel{model("01", "organization")}) {
		t.Fatalf("model fingerprint must detect type definition change")
	}

	baseAsserts := map[string][]*openfgav1.Assertion{
		"01": {assertion("document:1", "viewer", "user:a", true)},
	}
	flipedAsserts := map[string][]*openfgav1.Assertion{
		"01": {assertion("document:1", "viewer", "user:a", false)},
	}
	if FingerprintAssertions(baseAsserts) == FingerprintAssertions(flipedAsserts) {
		t.Fatalf("assertion fingerprint must detect expectation flip")
	}
}

func cloneTuples(in []*openfgav1.TupleKey) []*openfgav1.TupleKey {
	out := make([]*openfgav1.TupleKey, 0, len(in))
	for _, tk := range in {
		out = append(out, cloneTupleKey(tk))
	}
	return out
}

func TestCloneSnapshotIsolation(t *testing.T) {
	ctx, _ := structpb.NewStruct(map[string]any{"k": "v"})
	original := &Snapshot{
		Store:  &openfgav1.Store{Id: "store-1", Name: "name"},
		Models: []*openfgav1.AuthorizationModel{model("01", "user")},
		Tuples: []*openfgav1.TupleKey{{
			Object:    "document:1",
			Relation:  "viewer",
			User:      "user:a",
			Condition: &openfgav1.RelationshipCondition{Name: "c", Context: ctx},
		}},
		Assertions: map[string][]*openfgav1.Assertion{
			"01": {assertion("document:1", "viewer", "user:a", true)},
		},
	}

	cloned := CloneSnapshot(original)

	// Mutate the original after cloning.
	original.Store.Name = "mutated"
	original.Models[0].TypeDefinitions[0].Type = "mutated"
	original.Tuples[0].Object = "document:999"
	original.Tuples[0].GetCondition().Name = "mutated"
	original.Tuples[0].GetCondition().GetContext().AsMap()["k"] = "mutated"
	original.Assertions["01"][0].Expectation = false

	if cloned.Store.GetName() != "name" {
		t.Fatalf("store header leaked: %q", cloned.Store.GetName())
	}
	if cloned.Models[0].GetTypeDefinitions()[0].GetType() != "user" {
		t.Fatalf("model leaked")
	}
	if cloned.Tuples[0].GetObject() != "document:1" {
		t.Fatalf("tuple leaked")
	}
	if cloned.Tuples[0].GetCondition().GetName() != "c" {
		t.Fatalf("condition leaked")
	}
	if cloned.Tuples[0].GetCondition().GetContext().AsMap()["k"] != "v" {
		t.Fatalf("condition context leaked: %v", cloned.Tuples[0].GetCondition().GetContext().AsMap())
	}
	if cloned.Assertions["01"][0].GetExpectation() != true {
		t.Fatalf("assertion leaked")
	}

	// Mutate the clone: original must stay intact.
	cloned.Store.Name = "clone"
	cloned.Tuples[0].Object = "document:777"
	if original.Store.GetName() != "mutated" || original.Tuples[0].GetObject() != "document:999" {
		t.Fatalf("clone mutated the original")
	}
}

func TestBuildManifest(t *testing.T) {
	snap := &Snapshot{
		Models: []*openfgav1.AuthorizationModel{model("01", "user"), model("02", "document")},
		Tuples: []*openfgav1.TupleKey{
			tuple("document:1", "viewer", "user:a", ""),
			tuple("document:1", "viewer", "user:b", ""),
		},
		Assertions: map[string][]*openfgav1.Assertion{
			"01": {assertion("user:x", "viewer", "user:y", true)},
			"02": {
				assertion("document:1", "viewer", "user:a", true),
				assertion("document:1", "viewer", "user:b", false),
			},
			"03": {},
		},
	}

	capturedAt := time.Now().UTC()
	m := BuildManifest(snap, capturedAt)
	if m.ModelsCount != 2 || m.LatestModelID != "02" {
		t.Fatalf("unexpected model manifest: %+v", m)
	}
	if m.TuplesCount != 2 || m.TuplesFingerprint == "" {
		t.Fatalf("unexpected tuple manifest: %+v", m)
	}
	if m.AssertionModels != 2 || m.AssertionsCount != 3 || m.AssertionsFingerprint == "" {
		t.Fatalf("unexpected assertion manifest: %+v", m)
	}
	if m.ModelsFingerprint == "" || !m.CapturedAt.Equal(capturedAt) {
		t.Fatalf("unexpected manifest: %+v", m)
	}
}

func TestSentinelMatching(t *testing.T) {
	err := &VerificationMismatchError{Mismatches: []VerificationMismatch{{
		Kind: "tuples", Property: "count", Expected: "2", Actual: "1",
	}}}
	if !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("mismatch error must match ErrVerificationFailed")
	}
	for _, target := range []error{
		ErrEntryNotFound, ErrEntryAlreadyExists, ErrRecoveryActive,
		ErrRecoveryNotFound, ErrSnapshotUnavailable, ErrStoreAlreadyActive,
	} {
		if !errors.Is(target, target) {
			t.Fatalf("sentinel %v must match itself", target)
		}
	}
}

func TestRecoveryStatusTerminal(t *testing.T) {
	if RecoveryRunning.Terminal() {
		t.Fatalf("RUNNING is non-terminal")
	}
	for _, s := range []RecoveryStatus{RecoveryCompleted, RecoveryFailed, RecoveryDiscarded} {
		if !s.Terminal() {
			t.Fatalf("%s must be terminal", s)
		}
	}
}
