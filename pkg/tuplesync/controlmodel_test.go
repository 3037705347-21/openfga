package tuplesync

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/typesystem"
)

func TestControlModelLoadsWithExpectedShape(t *testing.T) {
	model := ControlModel()
	require.Equal(t, typesystem.SchemaVersion1_1, model.GetSchemaVersion())

	ts, err := typesystem.New(model)
	require.NoError(t, err)

	for _, typ := range []string{
		controlClaimType, controlSourceType, controlStoreType,
		controlPartType, controlRunType, controlSysType, controlLeaseType,
	} {
		td, ok := ts.GetTypeDefinition(typ)
		require.True(t, ok, "reserved type %s must exist", typ)
		require.NotNil(t, td)
	}
	_, ok := ts.GetTypeDefinition("not_a_type")
	require.False(t, ok)

	claimTD, _ := ts.GetTypeDefinition(controlClaimType)
	require.ElementsMatch(t, []string{relOwner, relTarget, relPart},
		relationNames(claimTD.GetRelations()))

	leaseTD, _ := ts.GetTypeDefinition(controlLeaseType)
	require.ElementsMatch(t, []string{relHolder}, relationNames(leaseTD.GetRelations()))

	sysTD, _ := ts.GetTypeDefinition(controlSysType)
	require.ElementsMatch(t, []string{relVersion}, relationNames(sysTD.GetRelations()))
}

func TestControlModelAcceptsLedgerTuples(t *testing.T) {
	// Every tuple shape the codec and bootstrap layers write must pass the
	// exact write validation enforced by the Write API.
	model := ControlModel()
	ts, err := typesystem.New(model)
	require.NoError(t, err)

	claim, err := newClaim("01HSTORE00000000000000000A", "team-directory", &openfgav1.TupleKey{
		Object:   "document:1",
		Relation: "viewer",
		User:     "group:eng#member",
	})
	require.NoError(t, err)
	keys := append(claim.encodeTuples(), versionMarkerTuple())

	for _, tk := range keys {
		require.NoError(t, validateTupleForModel(ts, tk), "ledger tuple must validate: %v", tk)
	}
}

func TestReservedTypeNames(t *testing.T) {
	for _, name := range ReservedTypeNames() {
		require.Contains(t, name, "fga_")
	}
	require.Contains(t, ReservedTypeNames(), controlClaimType)
}
