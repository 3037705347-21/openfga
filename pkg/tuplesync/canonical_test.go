package tuplesync

import (
	"encoding/json"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func mustStruct(t *testing.T, v map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(v)
	require.NoError(t, err)
	return s
}

func TestCanonicalKeyHashStableAcrossContextFieldOrder(t *testing.T) {
	// Two structs whose map iteration / construction order differs but whose
	// JSON content is identical must produce the same digest.
	ctxA := mustStruct(t, map[string]any{"b": "2", "a": "1"})
	ctxB := mustStruct(t, map[string]any{"a": "1", "b": "2"})

	k1, err := newCanonicalKey("01HSTORE", &openfgav1.TupleKey{
		Object:    "document:1",
		Relation:  "viewer",
		User:      "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "inOffice", Context: ctxA},
	})
	require.NoError(t, err)
	k2, err := newCanonicalKey("01HSTORE", &openfgav1.TupleKey{
		Object:    "document:1",
		Relation:  "viewer",
		User:      "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "inOffice", Context: ctxB},
	})
	require.NoError(t, err)

	require.Equal(t, k1.Payload(), k2.Payload())
	require.Equal(t, k1.Hash(), k2.Hash())
}

func TestCanonicalKeyDistinguishesAllDimensions(t *testing.T) {
	base := func(mut func(*openfgav1.TupleKey)) *openfgav1.TupleKey {
		tk := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:alice",
			Condition: &openfgav1.RelationshipCondition{
				Name:    "inOffice",
				Context: mustStruct(t, map[string]any{"ip": "10.0.0.1"}),
			},
		}
		mut(tk)
		return tk
	}

	reference, err := newCanonicalKey("01HSTORE", base(func(tk *openfgav1.TupleKey) {}))
	require.NoError(t, err)

	cases := map[string]*openfgav1.TupleKey{
		"different-store": base(func(tk *openfgav1.TupleKey) {}),
		"different-object": base(func(tk *openfgav1.TupleKey) {
			tk.Object = "document:2"
		}),
		"different-relation": base(func(tk *openfgav1.TupleKey) {
			tk.Relation = "editor"
		}),
		"different-user": base(func(tk *openfgav1.TupleKey) {
			tk.User = "user:bob"
		}),
		"different-condition-name": base(func(tk *openfgav1.TupleKey) {
			tk.Condition.Name = "atHome"
		}),
		"different-condition-context": base(func(tk *openfgav1.TupleKey) {
			tk.Condition.Context = mustStruct(t, map[string]any{"ip": "10.0.0.2"})
		}),
		"condition-removed": base(func(tk *openfgav1.TupleKey) {
			tk.Condition = nil
		}),
	}
	for name, tk := range cases {
		t.Run(name, func(t *testing.T) {
			store := "01HSTORE"
			if name == "different-store" {
				store = "01OTHER"
			}
			k, err := newCanonicalKey(store, tk)
			require.NoError(t, err)
			require.NotEqual(t, reference.Hash(), k.Hash(), "digest must differ for %s", name)
		})
	}
}

func TestCanonicalKeyHashFormat(t *testing.T) {
	k, err := newCanonicalKey("01HSTORE", &openfgav1.TupleKey{
		Object:   "document:1",
		Relation: "viewer",
		User:     "user:alice",
	})
	require.NoError(t, err)
	require.Len(t, k.Hash(), 64)
	require.Regexp(t, `^[0-9a-f]{64}$`, k.Hash())
}

func TestCanonicalPayloadRoundTripsThroughJSON(t *testing.T) {
	tk := &openfgav1.TupleKey{
		Object:   "document:1",
		Relation: "viewer",
		User:     "group:eng#member",
		Condition: &openfgav1.RelationshipCondition{
			Name:    "inOffice",
			Context: mustStruct(t, map[string]any{"floor": 3.0, "allowed": true}),
		},
	}
	k, err := newCanonicalKey("01HSTORE", tk)
	require.NoError(t, err)

	var got tuplePayload
	require.NoError(t, json.Unmarshal([]byte(k.Payload()), &got))
	require.Equal(t, tuplePayload{
		Object:   "document:1",
		Relation: "viewer",
		User:     "group:eng#member",
		Condition: &conditionPayload{
			Name:    "inOffice",
			Context: json.RawMessage(`{"allowed":true,"floor":3}`),
		},
	}, got)
}

func TestCanonicalPayloadOmitsCondition(t *testing.T) {
	k, err := newCanonicalKey("01HSTORE", &openfgav1.TupleKey{
		Object:   "document:1",
		Relation: "viewer",
		User:     "user:alice",
	})
	require.NoError(t, err)
	var got tuplePayload
	require.NoError(t, json.Unmarshal([]byte(k.Payload()), &got))
	require.Nil(t, got.Condition)
}
