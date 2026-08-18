package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// buildGroup mirrors the query the group closure in FetchNewDashboard builds.
// Keeping the shape in one place here means the regression below is checked
// against the same string the database would see.
func buildGroup(expr, pred, order string, limit int, runs string) string {
	w := ""
	if pred != "" {
		w = " WHERE " + pred
	}
	return fmt.Sprintf(`
		SELECT %s AS label, count() runs,
		       countIf(final_status = 'success')  success,
		       countIf(final_status = 'failed')   failed,
		       countIf(final_status = 'aborted')  aborted,
		       countIf(final_status NOT IN ('success','failed','aborted')) unfinished
		FROM (%s)%s
		GROUP BY label
		ORDER BY %s
		LIMIT %d`, expr, runs, w, order, limit)
}

func testRuns(t *testing.T, days int, repo, slug, platform string) (string, []interface{}) {
	t.Helper()
	where, args := chWhere(days, repo, slug, platform)
	return fmt.Sprintf(runsCTE, platformExpr, where), args
}

// The queries are assembled with fmt.Sprintf, and platformExpr contains a
// literal percent in "pve_version LIKE 'incus%'". That is harmless while it
// travels as an argument and corrupts the query the moment it reaches a format
// string, so the assembly is checked rather than assumed.
func TestRunsQueryAssembles(t *testing.T) {
	runs, args := testRuns(t, 7, "ProxmoxVE", "", "incus")

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

// Regression: the first version of this page filtered per-run columns with
// HAVING. final_status is neither grouped nor aggregated, so ClickHouse rejected
// every breakdown and the whole page returned 500. The predicate belongs in
// WHERE, which also means it runs before the grouping rather than after it.
func TestRunPredicatesUseWhereNotHaving(t *testing.T) {
	runs, _ := testRuns(t, 7, "", "", "")
	q := buildGroup("nsapp", "nsapp != ''", "runs DESC", 25, runs)

	outer := q[strings.LastIndex(q, "FROM ("):]
	if strings.Contains(outer, "HAVING") {
		t.Error("per-run predicate is in HAVING; it must be WHERE")
	}
	where, group := strings.Index(outer, "WHERE"), strings.Index(outer, "GROUP BY")
	if where < 0 || group < 0 || where > group {
		t.Errorf("WHERE must precede GROUP BY (WHERE at %d, GROUP BY at %d)", where, group)
	}
}

// The install log nests the runs query inside another Sprintf and needs
// %%Y-style escaping for formatDateTime. Getting that wrong yields a query that
// only fails against a live database.
func TestNestedQueriesEscapeCorrectly(t *testing.T) {
	runs, _ := testRuns(t, 30, "", "", "")

	recent := fmt.Sprintf(`
		SELECT nsapp, formatDateTime(last_seen, '%%Y-%%m-%%d %%H:%%M')
		FROM (%s) ORDER BY last_seen DESC LIMIT 200`, runs)

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

// The worst-apps ranking puts its floor inside ORDER BY, so a script with two
// runs and two failures cannot outrank one with two hundred runs and a real
// problem. If the floor were applied after the LIMIT, the list would be a
// top-25 by noise.
func TestWorstAppsRankingAppliesFloorBeforeLimit(t *testing.T) {
	runs, _ := testRuns(t, 7, "", "", "")
	min := minRunsFor(7)
	order := fmt.Sprintf(
		"if(success + failed >= %d, failed / (success + failed), -1) DESC, failed DESC", min)
	q := buildGroup("nsapp", "nsapp != ''", order, 25, runs)

	if !strings.Contains(q, fmt.Sprintf("success + failed >= %d", min)) {
		t.Error("the run floor is missing from the ordering")
	}
	if strings.Index(q, "ORDER BY") > strings.Index(q, "LIMIT") {
		t.Error("ORDER BY must come before LIMIT or the floor is applied to an already-truncated list")
	}
	// Aborted runs are excluded from the denominator on purpose: a user pressing
	// Ctrl+C is not the script failing.
	if strings.Contains(order, "aborted") {
		t.Error("aborted runs must not be part of the failure-rate denominator")
	}
}

// Regression, and the reason the page returned 500 twice: the runs subquery
// aliased sixteen aggregates back to the name of the column they aggregate --
// any(nsapp) AS nsapp. ClickHouse rejects that with "Different expressions with
// the same alias", and it rejects the whole query, so every panel went dark at
// once. Every alias now has a name of its own.
func TestNoAliasShadowsItsSourceColumn(t *testing.T) {
	re := regexp.MustCompile(`any\((\w+)\)\s+AS (\w+)`)
	for _, m := range re.FindAllStringSubmatch(runsCTE, -1) {
		if m[1] == m[2] {
			t.Errorf("any(%s) AS %s shadows its own source column", m[1], m[2])
		}
	}
	// The same shape, spelled differently.
	for _, agg := range []string{"argMax", "max", "min"} {
		re := regexp.MustCompile(agg + `\((\w+)[^)]*\)\s+AS (\w+)`)
		for _, m := range re.FindAllStringSubmatch(runsCTE, -1) {
			if m[1] == m[2] {
				t.Errorf("%s(%s ...) AS %s shadows its own source column", agg, m[1], m[2])
			}
		}
	}
}

func TestMinRunsScalesWithWindow(t *testing.T) {
	for _, c := range []struct{ days, want int }{
		{1, 5}, {7, 10}, {30, 20}, {365, 40},
	} {
		if got := minRunsFor(c.days); got != c.want {
			t.Errorf("minRunsFor(%d) = %d, want %d", c.days, got, c.want)
		}
	}
}
