package tuplesync

import (
	"strings"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func mustRoster(t *testing.T, storeID, source, body string) *Roster {
	t.Helper()
	if body == "" {
		body = "[]"
	}
	doc := `{"store_id":"` + storeID + `","source":"` + source + `","tuples":` + body + `}`
	r, issues, err := ParseRoster(strings.NewReader(doc))
	require.NoError(t, err)
	require.Empty(t, issues, "roster fixture must be valid: %v", issues)
	return r
}

func entryKey(object, relation, user string) RosterEntry {
	return RosterEntry{Object: object, Relation: relation, User: user}
}

// seedClaim writes one ownership claim (and optionally its data tuple),
// simulating the result of an earlier successful sync.
func seedClaim(t *testing.T, h *testHarness, controlID, storeID, source string, tk *openfgav1.TupleKey, withData bool) *Claim {
	t.Helper()
	c, err := newClaim(storeID, source, tk)
	require.NoError(t, err)
	require.NoError(t, h.ds.Write(h.ctx, controlID, nil, c.encodeTuples()))
	if withData {
		h.writeTargetTuples(storeID, tk)
	}
	return c
}

func planFor(t *testing.T, h *testHarness, roster *Roster) *Plan {
	t.Helper()
	before := len(h.allTuples(roster.StoreID))
	controlID := h.ensureControlStoreFor(roster.StoreID)
	beforeControl := len(h.allTuples(controlID))
	p := NewPlanner(h.backend)
	plan, err := p.Plan(h.ctx, roster)
	require.NoError(t, err)
	require.Equal(t, before, len(h.allTuples(roster.StoreID)), "Plan must never write target tuples")
	require.Equal(t, beforeControl, len(h.allTuples(controlID)), "Plan must never write control tuples")
	return plan
}

func requireIssue(t *testing.T, plan *Plan, code IssueCode, key string) Issue {
	t.Helper()
	for _, i := range plan.Issues {
		if i.Code == code && i.TupleKey == key {
			return i
		}
	}
	require.Failf(t, "missing issue", "want code=%s key=%s, got=%v", code, key, plan.Issues)
	return Issue{}
}

func writeIDs(ws []*openfgav1.TupleKey) []string {
	out := make([]string, 0, len(ws))
	for _, w := range ws {
		out = append(out, stringID(w.GetObject(), w.GetRelation(), w.GetUser()))
	}
	return out
}

func deleteIDs(ds []*openfgav1.TupleKeyWithoutCondition) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, stringID(d.GetObject(), d.GetRelation(), d.GetUser()))
	}
	return out
}

func stringID(o, r, u string) string { return o + "#" + r + "@" + u }

func TestPlanFreshStoreAddsEverything(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	roster := mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"},
		{"object":"document:1","relation":"editor","user":"user:bob"}
	]`)

	plan := planFor(t, h, roster)
	require.False(t, plan.Blocked)
	require.ElementsMatch(t,
		[]string{"document:1#viewer@user:alice", "document:1#editor@user:bob"}, writeIDs(plan.DataWrites))
	require.Empty(t, plan.DataDeletes)
	require.Empty(t, plan.Rewrites)
	require.Len(t, plan.AddClaims, 2)
	require.Empty(t, plan.RemoveClaims)
	require.Equal(t, 2, plan.PlannedWrites)
}

func TestPlanConvergedIsNoop(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	tk := &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}
	seedClaim(t, h, controlID, storeID, "dir", tk, true)

	plan := planFor(t, h, mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"}
	]`))
	require.False(t, plan.Blocked)
	require.Empty(t, plan.DataWrites)
	require.Empty(t, plan.DataDeletes)
	require.Empty(t, plan.Rewrites)
	require.Empty(t, plan.AddClaims)
	require.Empty(t, plan.RemoveClaims)
}

func TestPlanRemovalDeletesDataAndReleasesClaim(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	seedClaim(t, h, controlID, storeID, "dir",
		&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}, true)
	seedClaim(t, h, controlID, storeID, "dir",
		&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"}, true)

	plan := planFor(t, h, mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"}
	]`))
	require.False(t, plan.Blocked)
	require.Equal(t, []string{"document:2#viewer@user:alice"}, deleteIDs(plan.DataDeletes))
	require.Len(t, plan.RemoveClaims, 1)
	require.Equal(t, 1, plan.PlannedDeletes)
}

func TestPlanRoleChangeDeletesOldAndAddsNew(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	seedClaim(t, h, controlID, storeID, "dir",
		&openfgav1.TupleKey{Object: "document:1", Relation: "editor", User: "user:alice"}, true)

	plan := planFor(t, h, mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"}
	]`))
	require.False(t, plan.Blocked)
	require.Equal(t, []string{"document:1#editor@user:alice"}, deleteIDs(plan.DataDeletes))
	require.Equal(t, []string{"document:1#viewer@user:alice"}, writeIDs(plan.DataWrites))
}

func TestPlanUnclaimedExistingBlocksAndMakesNoPlan(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	h.writeTargetTuples(storeID,
		&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
		&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"})

	roster := mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"},
		{"object":"document:2","relation":"viewer","user":"user:alice"}
	]`)
	plan := planFor(t, h, roster)
	require.True(t, plan.Blocked)
	requireIssue(t, plan, IssueUnclaimedExisting, "document:1#viewer@user:alice")
	requireIssue(t, plan, IssueUnclaimedExisting, "document:2#viewer@user:alice")
	require.Nil(t, plan.DataWrites)
	require.Nil(t, plan.AddClaims)
}

func TestPlanForeignSourceBlocked(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	seedClaim(t, h, controlID, storeID, "team-A",
		&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}, true)

	plan := planFor(t, h, mustRoster(t, storeID, "team-B", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"}
	]`))
	require.True(t, plan.Blocked)
	i := requireIssue(t, plan, IssueForeignSource, "document:1#viewer@user:alice")
	require.Equal(t, "team-A", i.Source)
	require.Nil(t, plan.DataWrites)
}

func TestPlanLedgerWithTwoOwnersBlocked(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	tk := &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}
	c := seedClaim(t, h, controlID, storeID, "team-A", tk, true)
	// corrupt: another owner tuple on the same claim object
	require.NoError(t, h.ds.Write(h.ctx, controlID, nil, []*openfgav1.TupleKey{
		{Object: c.object(), Relation: relOwner, User: "fga_source:team-B"},
	}))

	plan := planFor(t, h, mustRoster(t, storeID, "team-A", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"}
	]`))
	require.True(t, plan.Blocked)
	var found bool
	for _, i := range plan.Issues {
		if i.Code == IssueLedgerUnverifiable && strings.HasPrefix(i.TupleKey, controlClaimType+":") {
			found = true
		}
	}
	require.True(t, found, "want ledger_unverifiable on claim object, got %v", plan.Issues)
}

func TestPlanCrashResidualClaimWithoutData(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	// claim exists (claim-first phase completed), data tuple never written.
	c := seedClaim(t, h, controlID, storeID, "dir",
		&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}, false)
	require.False(t, h.tupleExists(storeID, "document:1", "viewer", "user:alice"))

	plan := planFor(t, h, mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"}
	]`))
	require.False(t, plan.Blocked, "%v", plan.Issues)
	require.Equal(t, []string{"document:1#viewer@user:alice"}, writeIDs(plan.DataWrites))
	require.Empty(t, plan.AddClaims, "claim already present: no duplicate claim write")
	_ = c
}

func TestPlanCrashResidualDataAlreadyDeleted(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	// data gone but claim not yet released.
	c := seedClaim(t, h, controlID, storeID, "dir",
		&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"}, false)

	plan := planFor(t, h, mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice"}
	]`))
	// document:1 is a plain new add as well
	require.False(t, plan.Blocked, "%v", plan.Issues)
	require.Len(t, plan.RemoveClaims, 1)
	require.Equal(t, c.hash(), plan.RemoveClaims[0].hash())
	require.NotContains(t, deleteIDs(plan.DataDeletes), "document:2#viewer@user:alice",
		"deleting an already-deleted tuple is unnecessary; the claim simply gets released")
	require.Equal(t, []string{"document:1#viewer@user:alice"}, writeIDs(plan.DataWrites))
}

func TestPlanConditionChangeProducesRewrite(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	oldTK := &openfgav1.TupleKey{
		Object: "document:1", Relation: "viewer", User: "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "inOffice", Context: mustStruct(t, map[string]any{"floor": 1.0})},
	}
	oldClaim := seedClaim(t, h, controlID, storeID, "dir", oldTK, true)

	newRoster := mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice",
		 "condition":{"name":"inOffice","context":{"floor":4}}}
	]`)
	plan := planFor(t, h, newRoster)
	require.False(t, plan.Blocked, "%v", plan.Issues)
	require.Len(t, plan.Rewrites, 1)
	require.Equal(t, "document:1#viewer@user:alice", stringID(
		plan.Rewrites[0].Delete.GetObject(), plan.Rewrites[0].Delete.GetRelation(), plan.Rewrites[0].Delete.GetUser()))
	require.Equal(t, float64(4), plan.Rewrites[0].Write.GetCondition().GetContext().AsMap()["floor"])

	// old claim released, new claim added
	var removed, added []string
	for _, c := range plan.RemoveClaims {
		removed = append(removed, c.hash())
	}
	for _, c := range plan.AddClaims {
		added = append(added, c.hash())
	}
	require.Contains(t, removed, oldClaim.hash())
	require.Len(t, added, 1)
	require.NotContains(t, added, oldClaim.hash())
}

func TestPlanConditionContextOrderingIsNoop(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	tk := &openfgav1.TupleKey{
		Object: "document:1", Relation: "viewer", User: "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "inOffice", Context: mustStruct(t, map[string]any{"floor": 3.0, "wing": 7.0})},
	}
	seedClaim(t, h, controlID, storeID, "dir", tk, true)

	plan := planFor(t, h, mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice",
		 "condition":{"name":"inOffice","context":{"wing":7,"floor":3}}}
	]`))
	require.False(t, plan.Blocked, "%v", plan.Issues)
	require.Empty(t, plan.Rewrites)
	require.Empty(t, plan.AddClaims)
	require.Empty(t, plan.RemoveClaims)
}

func TestPlanDoubleClaimAfterRewriteCrashConverges(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)

	// old condition claimed and present in store; new condition claim added
	// but data rewrite never happened (mid-rewrite crash).
	oldTK := &openfgav1.TupleKey{
		Object: "document:1", Relation: "viewer", User: "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "inOffice", Context: mustStruct(t, map[string]any{"floor": 1.0})},
	}
	newTK := &openfgav1.TupleKey{
		Object: "document:1", Relation: "viewer", User: "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "inOffice", Context: mustStruct(t, map[string]any{"floor": 4.0})},
	}
	oldSeeded := seedClaim(t, h, controlID, storeID, "dir", oldTK, true)
	newSeeded := seedClaim(t, h, controlID, storeID, "dir", newTK, false)

	plan := planFor(t, h, mustRoster(t, storeID, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice",
		 "condition":{"name":"inOffice","context":{"floor":4}}}
	]`))
	require.False(t, plan.Blocked, "%v", plan.Issues)
	require.Len(t, plan.Rewrites, 1)
	var removed []string
	for _, c := range plan.RemoveClaims {
		removed = append(removed, c.hash())
	}
	require.Contains(t, removed, oldSeeded.hash())
	require.NotContains(t, removed, newSeeded.hash())

	// other side of the crash: data already rewritten, only stale claim left
	h2 := newTestHarness(t)
	s2, _ := h2.createTargetStore()
	c2 := h2.ensureControlStoreFor(s2)
	seedClaim(t, h2, c2, s2, "dir", oldTK, false)
	seedClaim(t, h2, c2, s2, "dir", newTK, true)
	plan2 := planFor(t, h2, mustRoster(t, s2, "dir", `[
		{"object":"document:1","relation":"viewer","user":"user:alice",
		 "condition":{"name":"inOffice","context":{"floor":4}}}
	]`))
	require.False(t, plan2.Blocked, "%v", plan2.Issues)
	require.Empty(t, plan2.Rewrites)
	require.Empty(t, plan2.DataWrites)
	require.Len(t, plan2.RemoveClaims, 1)
	oldClaimInS2, err := newClaim(s2, "dir", oldTK)
	require.NoError(t, err)
	require.Equal(t, oldClaimInS2.hash(), plan2.RemoveClaims[0].hash())
}

func TestPlanModelValidationBlocked(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()

	cases := map[string]string{
		"unknown type":      `{"object":"widget:1","relation":"viewer","user":"user:alice"}`,
		"unknown relation":  `{"object":"document:1","relation":"owner","user":"user:alice"}`,
		"wrong user type":   `{"object":"document:1","relation":"viewer","user":"document:2"}`,
		"unknown condition": `{"object":"document:1","relation":"viewer","user":"user:alice","condition":{"name":"nope"}}`,
	}
	expected := map[string]string{
		"unknown type":      "widget:1#viewer@user:alice",
		"unknown relation":  "document:1#owner@user:alice",
		"wrong user type":   "document:1#viewer@document:2",
		"unknown condition": "document:1#viewer@user:alice",
	}
	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			plan := planFor(t, h, mustRoster(t, storeID, "dir", "["+entry+"]"))
			require.True(t, plan.Blocked)
			requireIssue(t, plan, IssueModelValidation, expected[name])
		})
	}
}

func TestPlanDuplicateAndMalformedStillReportAll(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	// one duplicate, one unknown relation, one fine tuple: all findings.
	roster, _, err := ParseRoster(strings.NewReader(`{"store_id":"` + storeID + `","source":"dir","tuples":[
		{"object":"document:1","relation":"viewer","user":"user:alice"},
		{"object":"document:1","relation":"viewer","user":"user:alice"},
		{"object":"document:1","relation":"nope","user":"user:bob"}
	]}`))
	require.NoError(t, err)
	plan := planFor(t, h, roster)
	require.True(t, plan.Blocked)
	requireIssue(t, plan, IssueDuplicateInRoster, "document:1#viewer@user:alice")
	requireIssue(t, plan, IssueModelValidation, "document:1#nope@user:bob")
	require.True(t, len(plan.Issues) >= 2)
}

func TestPlanAdopt(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	h.writeTargetTuples(storeID, &openfgav1.TupleKey{
		Object: "document:1", Relation: "viewer", User: "user:alice",
		Condition: &openfgav1.RelationshipCondition{Name: "inOffice", Context: mustStruct(t, map[string]any{"floor": 9.0})},
	})
	seedClaim(t, h, controlID, storeID, "dir",
		&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:bob"}, true)

	planner := NewPlanner(h.backend)
	plan, err := planner.PlanAdopt(h.ctx, storeID, "dir", []RosterEntry{
		entryKey("document:1", "viewer", "user:alice"),
		entryKey("document:3", "viewer", "user:ghost"),
		entryKey("document:2", "viewer", "user:bob"),
	})
	require.NoError(t, err)
	require.True(t, plan.Blocked)
	requireIssue(t, plan, IssueAdoptNotFound, "document:3#viewer@user:ghost")
	requireIssue(t, plan, IssueAdoptAlreadyClaimed, "document:2#viewer@user:bob")
	require.Empty(t, plan.AddClaims, "blocked adopt must plan no claims")

	// only the existing unclaimed tuple succeeds and captures its condition.
	ok, err := planner.PlanAdopt(h.ctx, storeID, "dir", []RosterEntry{
		entryKey("document:1", "viewer", "user:alice"),
	})
	require.NoError(t, err)
	require.False(t, ok.Blocked)
	require.Len(t, ok.AddClaims, 1)
	tk, err := ok.AddClaims[0].tupleKey()
	require.NoError(t, err)
	require.Equal(t, "inOffice", tk.GetCondition().GetName())
	require.Equal(t, float64(9), tk.GetCondition().GetContext().AsMap()["floor"])
}

func TestPlanOtherSourceClaimsAreInvisible(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	controlID := h.ensureControlStoreFor(storeID)
	// another source manages doc:1; our roster manages doc:2
	seedClaim(t, h, controlID, storeID, "other",
		&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}, true)
	seedClaim(t, h, controlID, storeID, "dir",
		&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"}, true)

	plan := planFor(t, h, mustRoster(t, storeID, "dir", `[
		{"object":"document:2","relation":"viewer","user":"user:alice"}
	]`))
	require.False(t, plan.Blocked, "%v", plan.Issues)
	require.Empty(t, plan.DataDeletes, "must not delete other sources' tuples")
	require.Empty(t, plan.RemoveClaims)
}
