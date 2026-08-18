package main

import (
	"fmt"
	"strings"
	"testing"
)

// The queries are assembled with fmt.Sprintf, and platformExpr contains a
// literal percent sign in "pve_version LIKE 'incus%'". That is harmless while it
// travels as an argument and corrupts the query the moment it reaches a format
// string, so the assembly is checked rather than assumed.
func TestRunsQueryAssembles(t *testing.T) {
	where, args := chWhere(7, "ProxmoxVE", "", "incus")
	runs := fmt.Sprintf(runsCTE, platformExpr, where)

	if strings.Contains(runs, "%!") {
		t.Fatalf("format verb left unconsumed in runs query:\n%s", runs)
	}
	for _, want := range []string{
		"argMax(status, created)",
		"GROUP BY run",
		"LIKE 'incus%'",
	} {
		if !strings.Contains(runs, want) {
			t.Errorf("runs query is missing %q", want)
		}
	}
	if len(args) == 0 {
		t.Error("expected at least the day-range argument to be bound")
	}
}

// The recent-runs query nests the runs query inside another Sprintf and needs
// %%Y-style escaping for formatDateTime. Getting that wrong yields a query that
// only fails against a live database.
func TestNestedQueriesEscapeCorrectly(t *testing.T) {
	where, _ := chWhere(30, "", "", "")
	runs := fmt.Sprintf(runsCTE, platformExpr, where)

	recent := fmt.Sprintf(`
		SELECT nsapp, formatDateTime(last_seen, '%%Y-%%m-%%d %%H:%%M')
		FROM (%s) ORDER BY last_seen DESC LIMIT 60`, runs)

	if strings.Contains(recent, "%!") {
		t.Fatalf("format verb left unconsumed:\n%s", recent)
	}
	if !strings.Contains(recent, "'%Y-%m-%d %H:%M'") {
		t.Error("formatDateTime pattern did not survive the nested Sprintf")
	}
	if !strings.Contains(recent, "argMax(status, created)") {
		t.Error("the runs subquery was not embedded")
	}
}

// Every breakdown goes through the same builder, so one wrong assumption there
// would be wrong fifteen times over.
func TestBreakdownQueryShape(t *testing.T) {
	where, _ := chWhere(1, "", "", "")
	runs := fmt.Sprintf(runsCTE, platformExpr, where)

	q := fmt.Sprintf(
		"SELECT %s AS label, count() c FROM (%s) GROUP BY label%s ORDER BY c DESC LIMIT %d",
		"final_cat", runs, " HAVING final_status = 'failed'", 15)

	if strings.Contains(q, "%!") {
		t.Fatalf("format verb left unconsumed:\n%s", q)
	}
	// HAVING has to sit between GROUP BY and ORDER BY, or ClickHouse rejects it.
	gb, hv, ob := strings.Index(q, "GROUP BY"), strings.Index(q, "HAVING"), strings.Index(q, "ORDER BY")
	if !(gb < hv && hv < ob) {
		t.Errorf("clause order is wrong: GROUP BY at %d, HAVING at %d, ORDER BY at %d", gb, hv, ob)
	}
}
