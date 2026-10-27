package tuplesync

import (
	"strings"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestParseRosterValid(t *testing.T) {
	r := `{
		"store_id": "01HSTORE",
		"source": "team-directory",
		"authorization_model_id": "01HMODEL",
		"tuples": [
			{"object": "document:1", "relation": "viewer", "user": "user:alice"},
			{"object": "document:1", "relation": "viewer", "user": "user:bob",
				"condition": {"name": "inOffice", "context": {"floor": 3}}}
		]
	}`
	roster, issues, err := ParseRoster(strings.NewReader(r))
	require.NoError(t, err)
	require.Empty(t, issues)
	require.Equal(t, "01HSTORE", roster.StoreID)
	require.Equal(t, "team-directory", roster.Source)
	require.Equal(t, "01HMODEL", roster.AuthorizationModelID)
	require.Len(t, roster.Tuples, 2)

	tk, err := roster.Tuples[1].toTupleKey()
	require.NoError(t, err)
	require.Equal(t, &openfgav1.TupleKey{
		Object:   "document:1",
		Relation: "viewer",
		User:     "user:bob",
		Condition: &openfgav1.RelationshipCondition{
			Name:    "inOffice",
			Context: mustStruct(t, map[string]any{"floor": 3.0}),
		},
	}, tk)
}

func TestParseRosterRejectsInvalidJSON(t *testing.T) {
	_, _, err := ParseRoster(strings.NewReader(`{"tuples": [`))
	require.Error(t, err)
}

func TestParseRosterMissingEnvelopeFields(t *testing.T) {
	// missing store_id and source, and an empty tuples list
	_, issues, err := ParseRoster(strings.NewReader(`{"tuples": []}`))
	require.NoError(t, err)
	require.Contains(t, issueCodes(issues), IssueMalformedRoster)
}

func TestParseRosterMalformedEntries(t *testing.T) {
	cases := map[string]string{
		"missing-object":               `{"relation":"viewer","user":"user:alice"}`,
		"object-missing-type":          `{"object":"1","relation":"viewer","user":"user:alice"}`,
		"object-with-space":            `{"object":"document: 1","relation":"viewer","user":"user:alice"}`,
		"object-with-hash":             `{"object":"doc#1:1","relation":"viewer","user":"user:alice"}`,
		"missing-relation":             `{"object":"document:1","user":"user:alice"}`,
		"relation-with-colon":          `{"object":"document:1","relation":"view:er","user":"user:alice"}`,
		"missing-user":                 `{"object":"document:1","relation":"viewer"}`,
		"user-with-space":              `{"object":"document:1","relation":"viewer","user":"user: ali"}`,
		"condition-without-name":       `{"object":"document:1","relation":"viewer","user":"user:alice","condition":{"context":{"a":1}}}`,
		"condition-empty-name":         `{"object":"document:1","relation":"viewer","user":"user:alice","condition":{"name":"","context":{}}}`,
		"condition-context-not-object": `{"object":"document:1","relation":"viewer","user":"user:alice","condition":{"name":"c","context":[1,2]}}`,
		"condition-context-scalar":     `{"object":"document:1","relation":"viewer","user":"user:alice","condition":{"name":"c","context":"nope"}}`,
	}
	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			doc := `{"store_id":"s","source":"src","tuples":[` + entry + `]}`
			_, issues, err := ParseRoster(strings.NewReader(doc))
			require.NoError(t, err)
			require.NotEmpty(t, issues)
			require.Equal(t, IssueMalformedTuple, issues[0].Code)
			require.NotEmpty(t, issues[0].Message)
		})
	}
}

func TestParseRosterDuplicates(t *testing.T) {
	t.Run("identical duplicate", func(t *testing.T) {
		doc := `{"store_id":"s","source":"src","tuples":[
			{"object":"document:1","relation":"viewer","user":"user:alice"},
			{"object":"document:1","relation":"viewer","user":"user:alice"}
		]}`
		_, issues, err := ParseRoster(strings.NewReader(doc))
		require.NoError(t, err)
		require.Equal(t, []IssueCode{IssueDuplicateInRoster}, issueCodes(issues))
		require.Equal(t, "document:1#viewer@user:alice", issues[0].TupleKey)
	})

	t.Run("duplicate with different condition", func(t *testing.T) {
		doc := `{"store_id":"s","source":"src","tuples":[
			{"object":"document:1","relation":"viewer","user":"user:alice","condition":{"name":"a"}},
			{"object":"document:1","relation":"viewer","user":"user:alice","condition":{"name":"b"}}
		]}`
		_, issues, err := ParseRoster(strings.NewReader(doc))
		require.NoError(t, err)
		require.Equal(t, []IssueCode{IssueDuplicateInRoster}, issueCodes(issues))
	})
}

func TestParseRosterIssuesAreDeterministic(t *testing.T) {
	doc := `{"store_id":"s","source":"src","tuples":[
		{"object":"document:2","relation":"viewer","user":"user:alice"},
		{"object":"document:2","relation":"viewer","user":"user:alice"},
		{"object":"document:1","relation":"viewer","user":"user:alice"},
		{"object":"document:1","relation":"viewer","user":"user:alice"}
	]}`
	first, issues1, err := ParseRoster(strings.NewReader(doc))
	require.NoError(t, err)
	second, issues2, err := ParseRoster(strings.NewReader(doc))
	require.NoError(t, err)
	require.Equal(t, issueStrings(issues1...), issueStrings(issues2...))
	require.Equal(t, issueStrings(first.Validate()...), issueStrings(second.Validate()...))
}

func TestRosterEntryTupleKeyString(t *testing.T) {
	e := RosterEntry{
		Object: "document:1", Relation: "viewer", User: "group:eng#member",
	}
	require.Equal(t, "document:1#viewer@group:eng#member", e.keyString())
}

func issueCodes(issues []Issue) []IssueCode {
	out := make([]IssueCode, 0, len(issues))
	for _, i := range issues {
		out = append(out, i.Code)
	}
	return out
}

func issueStrings(issues ...Issue) []string {
	out := make([]string, 0, len(issues))
	for _, i := range issues {
		out = append(out, string(i.Code)+":"+i.TupleKey)
	}
	return out
}
