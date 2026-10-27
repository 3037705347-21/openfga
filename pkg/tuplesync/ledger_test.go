package tuplesync

import (
	"strings"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func claimTuplesForTest(t *testing.T, c *Claim) []*openfgav1.Tuple {
	t.Helper()
	keys := c.encodeTuples()
	out := make([]*openfgav1.Tuple, 0, len(keys))
	for _, k := range keys {
		out = append(out, &openfgav1.Tuple{Key: k})
	}
	return out
}

func TestClaimEncodeDecodeRoundTrip(t *testing.T) {
	cases := map[string]*openfgav1.TupleKey{
		"plain": {
			Object: "document:1", Relation: "viewer", User: "user:alice",
		},
		"userset-user": {
			Object: "document:1", Relation: "viewer", User: "group:eng#member",
		},
		"typed-wildcard": {
			Object: "document:1", Relation: "viewer", User: "user:*",
		},
		"condition-with-context": {
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:alice",
			Condition: &openfgav1.RelationshipCondition{
				Name:    "inOffice",
				Context: mustStruct(t, map[string]any{"floor": 3.0}),
			},
		},
		"condition-without-context": {
			Object:    "document:1",
			Relation:  "viewer",
			User:      "user:alice",
			Condition: &openfgav1.RelationshipCondition{Name: "grantedByBatch"},
		},
		"huge-context": {
			Object:   "document:big",
			Relation: "viewer",
			User:     "user:charlie",
			Condition: &openfgav1.RelationshipCondition{
				Name:    "inIpRange",
				Context: mustStruct(t, map[string]any{"ranges": bigStringList(400, 64)}),
			},
		},
	}
	for name, tk := range cases {
		t.Run(name, func(t *testing.T) {
			claim, err := newClaim("01HSTORE00000000000000000A", "team-directory", tk)
			require.NoError(t, err)

			encoded := claimTuplesForTest(t, claim)
			decoded, err := decodeClaimObject(claim.object(), encoded)
			require.NoError(t, err)

			require.Equal(t, claim.StoreID, decoded.StoreID)
			require.Equal(t, claim.Source, decoded.Source)
			require.Equal(t, claim.object(), decoded.object())

			got, err := decoded.tupleKey()
			require.NoError(t, err)
			require.Equal(t, tk.String(), got.String())
			requireProtoEqual(t, tk, got)
		})
	}
}

func TestClaimEncodingIsDeterministic(t *testing.T) {
	tk := &openfgav1.TupleKey{
		Object:    "document:1",
		Relation:  "viewer",
		User:      "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "c", Context: mustStruct(t, map[string]any{"b": 2.0, "a": 1.0})},
	}
	c, err := newClaim("store1", "src", tk)
	require.NoError(t, err)
	first := c.encodeTuples()
	second := c.encodeTuples()
	require.Equal(t, tupleKeyStrings(first), tupleKeyStrings(second))
}

func TestClaimEncodingRespectsMySQLEnvelope(t *testing.T) {
	tk := &openfgav1.TupleKey{
		Object:    "document:big",
		Relation:  "viewer",
		User:      "user:charlie",
		Condition: &openfgav1.RelationshipCondition{Name: "c", Context: mustStruct(t, map[string]any{"ranges": bigStringList(400, 64)})},
	}
	claim, err := newClaim("01HSTORE00000000000000000A", strings.Repeat("s", maxSourceIDLength), tk)
	require.NoError(t, err)

	encoded := claim.encodeTuples()
	require.Greater(t, len(encoded), 3, "10KB context must span multiple part tuples")
	for _, k := range encoded {
		objectType, objectID := splitObjectForTest(k.GetObject())
		require.LessOrEqual(t, len(objectType), mysqlMaxObjectTypeLength, "object type: %s", k.GetObject())
		require.LessOrEqual(t, len(objectID), mysqlMaxObjectIDLength, "object id: %s", k.GetObject())
		require.LessOrEqual(t, len(k.GetRelation()), mysqlMaxRelationLength, "relation: %s", k.GetRelation())
		require.LessOrEqual(t, len(k.GetUser()), mysqlMaxUserLength, "user: %s", k.GetUser())
	}

	// round trip through the huge encoding still works
	decoded, err := decodeClaimObject(claim.object(), claimTuplesForTest(t, claim))
	require.NoError(t, err)
	got, err := decoded.tupleKey()
	require.NoError(t, err)
	requireProtoEqual(t, tk, got)
}

func TestClaimHashBindsStoreAndTuple(t *testing.T) {
	tk := &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}
	base, err := newClaim("storeA", "src", tk)
	require.NoError(t, err)

	otherStore, err := newClaim("storeB", "src", tk)
	require.NoError(t, err)
	require.NotEqual(t, base.hash(), otherStore.hash())

	otherTuple, err := newClaim("storeA", "src", &openfgav1.TupleKey{
		Object: "document:2", Relation: "viewer", User: "user:alice",
	})
	require.NoError(t, err)
	require.NotEqual(t, base.hash(), otherTuple.hash())

	// source identity is NOT part of the hash: ownership is unique per tuple
	// regardless of which source claims it.
	otherSource, err := newClaim("storeA", "src2", tk)
	require.NoError(t, err)
	require.Equal(t, base.hash(), otherSource.hash())
}

func TestDecodeClaimFailures(t *testing.T) {
	tk := &openfgav1.TupleKey{
		Object:    "document:1",
		Relation:  "viewer",
		User:      "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "c", Context: mustStruct(t, map[string]any{"a": 1.0})},
	}
	base, err := newClaim("01HSTORE00000000000000000A", "team-directory", tk)
	require.NoError(t, err)
	obj := base.object()

	t.Run("two distinct owners", func(t *testing.T) {
		tuples := claimTuplesForTest(t, base)
		tuples = append(tuples, &openfgav1.Tuple{Key: &openfgav1.TupleKey{
			Object: obj, Relation: relOwner, User: "fga_source:other-source",
		}})
		_, err := decodeClaimObject(obj, tuples)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
		require.Contains(t, err.Error(), obj)
	})

	t.Run("missing owner", func(t *testing.T) {
		var tuples []*openfgav1.Tuple
		for _, tp := range claimTuplesForTest(t, base) {
			if tp.GetKey().GetRelation() == relOwner {
				continue
			}
			tuples = append(tuples, tp)
		}
		_, err := decodeClaimObject(obj, tuples)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})

	t.Run("missing target", func(t *testing.T) {
		var tuples []*openfgav1.Tuple
		for _, tp := range claimTuplesForTest(t, base) {
			if tp.GetKey().GetRelation() == relTarget {
				continue
			}
			tuples = append(tuples, tp)
		}
		_, err := decodeClaimObject(obj, tuples)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})

	t.Run("gap in chunk indexes", func(t *testing.T) {
		var tuples []*openfgav1.Tuple
		for _, tp := range claimTuplesForTest(t, base) {
			k := tp.GetKey()
			if k.GetRelation() == relPart {
				idx, _, ok := parsePartUser(k.GetUser())
				require.True(t, ok)
				if idx == 0 {
					continue
				}
			}
			tuples = append(tuples, tp)
		}
		_, err := decodeClaimObject(obj, tuples)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})

	t.Run("duplicate chunk index", func(t *testing.T) {
		tuples := claimTuplesForTest(t, base)
		for _, tp := range tuples {
			if tp.GetKey().GetRelation() == relPart {
				tuples = append(tuples, &openfgav1.Tuple{Key: tp.GetKey()})
				break
			}
		}
		_, err := decodeClaimObject(obj, tuples)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})

	t.Run("corrupt payload base64", func(t *testing.T) {
		tuples := claimTuplesForTest(t, base)
		for _, tp := range tuples {
			if tp.GetKey().GetRelation() == relPart {
				tp.GetKey().User = "fga_pc:" + base.hash() + "-0-###notb64###"
				break
			}
		}
		_, err := decodeClaimObject(obj, tuples)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})

	t.Run("tampered payload mismatches hash", func(t *testing.T) {
		// Rebuild claim tuples from a DIFFERENT tuple, but present them under
		// the original claim object: the recomputed hash must not match.
		other, err := newClaim(base.StoreID, base.Source, &openfgav1.TupleKey{
			Object: "document:2", Relation: "viewer", User: "user:alice",
		})
		require.NoError(t, err)
		var tuples []*openfgav1.Tuple
		for _, tp := range claimTuplesForTest(t, other) {
			k := &openfgav1.TupleKey{
				Object:   obj,
				Relation: tp.GetKey().GetRelation(),
				User:     tp.GetKey().GetUser(),
			}
			if k.GetRelation() == relPart {
				// retarget the part object hash back to the original object
				idx, payload, ok := parsePartUser(tp.GetKey().GetUser())
				require.True(t, ok)
				k.User = formatPartUser(base.hash(), idx, payload)
			}
			tuples = append(tuples, &openfgav1.Tuple{Key: k})
		}
		_, err = decodeClaimObject(obj, tuples)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
		require.Contains(t, err.Error(), "digest mismatch")
	})

	t.Run("no tuples at all", func(t *testing.T) {
		_, err := decodeClaimObject(obj, nil)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})

	t.Run("foreign object rejected", func(t *testing.T) {
		tuples := claimTuplesForTest(t, base)
		tuples[0].GetKey().Object = "fga_claim:deadbeef"
		_, err := decodeClaimObject(obj, tuples)
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})
}

func TestRosterSourceIDValidation(t *testing.T) {
	// source is embedded verbatim in fga_source:<source> ledger users and
	// must fit both the charset and the MySQL user column envelope.
	long := strings.Repeat("a", maxSourceIDLength+1)
	r := &Roster{StoreID: "s", Source: long, Tuples: []RosterEntry{
		{Object: "document:1", Relation: "viewer", User: "user:alice"},
	}}
	issues := r.Validate()
	require.Contains(t, issueCodes(issues), IssueMalformedRoster)

	bad := &Roster{StoreID: "s", Source: "bad source!", Tuples: []RosterEntry{
		{Object: "document:1", Relation: "viewer", User: "user:alice"},
	}}
	require.Contains(t, issueCodes(bad.Validate()), IssueMalformedRoster)
}

func requireProtoEqual(t *testing.T, want, got *openfgav1.TupleKey) {
	t.Helper()
	require.Equal(t, want.GetObject(), got.GetObject())
	require.Equal(t, want.GetRelation(), got.GetRelation())
	require.Equal(t, want.GetUser(), got.GetUser())
	require.Equal(t, want.GetCondition().GetName(), got.GetCondition().GetName())
	require.Equal(t, want.GetCondition().GetContext().AsMap(), got.GetCondition().GetContext().AsMap())
}

func tupleKeyStrings(keys []*openfgav1.TupleKey) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k.String())
	}
	return out
}

func splitObjectForTest(s string) (string, string) {
	for i, r := range s {
		if r == ':' {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}

func bigStringList(n, size int) []any {
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, strings.Repeat("x", size))
	}
	return out
}
