package store

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// roles names the PostgreSQL role each igs component runs as when the
// platform gives it its own user (REQ-008 fix 22b; PostgreSQL at 200
// connections, D-037). The scheduler and the migrate hook keep the owner
// (igs): the scheduler's maintenance creates and drops partitions, which
// only the tables' owner may do. The normalizer uses no PostgreSQL. The names are the platform's (<service><role>): DigitalOcean
// allows only lowercase letters, digits, "-" and "." in user names.
var roles = map[string]string{
	"api":           "igsapi",
	"crawler":       "igscrawler",
	"persister":     "igspersister",
	"media":         "igsmedia",
	"registry-sync": "igsregistrysync",
}

// RoleOf returns the role a component runs as, or "" for the owner.
func RoleOf(component string) string { return roles[component] }

// RoleNames lists every per-component role, sorted.
func RoleNames() []string {
	out := make([]string, 0, len(roles))
	for _, r := range roles {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}

// RoleGrants returns what a role may do, table by table, sorted by table.
func RoleGrants(role string) [][2]string {
	var out [][2]string
	for t, privs := range grants[role] {
		out = append(out, [2]string{t, strings.Join(privs, ", ")})
	}
	slices.SortFunc(out, func(a, b [2]string) int { return strings.Compare(a[0], b[0]) })
	return out
}

// ApplyRoleGrants gives each per-component role that exists exactly its
// privileges on schema ig: everything else on the schema's tables is
// revoked first, so a privilege dropped from the list is taken away too.
// A role that doesn't exist is left out: the platform creates the users,
// and until it does the components share the owner. It runs as the
// tables' owner (igsctl migrate up) and returns the roles it granted.
func ApplyRoleGrants(ctx context.Context, db *pgxpool.Pool) ([]string, error) {
	var done []string
	for _, role := range RoleNames() {
		var exists bool
		if err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
			return done, err
		}
		if !exists {
			continue
		}
		err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			r := pgx.Identifier{role}.Sanitize()
			schema := pgx.Identifier{IGSchema}.Sanitize()
			stmts := []string{
				"REVOKE ALL ON ALL TABLES IN SCHEMA " + schema + " FROM " + r,
				"REVOKE ALL ON ALL SEQUENCES IN SCHEMA " + schema + " FROM " + r,
				"REVOKE ALL ON ALL FUNCTIONS IN SCHEMA " + schema + " FROM " + r,
				"GRANT USAGE ON SCHEMA " + schema + " TO " + r,
			}
			for _, fn := range functions[role] {
				stmts = append(stmts, "GRANT EXECUTE ON FUNCTION "+schema+"."+fn+" TO "+r)
			}
			inserts := false
			for table, privs := range grants[role] {
				stmts = append(stmts, fmt.Sprintf("GRANT %s ON TABLE %s TO %s",
					strings.Join(privs, ", "), pgx.Identifier{IGSchema, table}.Sanitize(), r))
				inserts = inserts || slices.Contains(privs, "INSERT")
			}
			if inserts {
				// Identity and serial columns draw from sequences.
				stmts = append(stmts, "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "+schema+" TO "+r)
			}
			for _, s := range stmts {
				if _, err := tx.Exec(ctx, s); err != nil {
					return fmt.Errorf("%s: %w", s, err)
				}
			}
			return nil
		})
		if err != nil {
			return done, fmt.Errorf("roles: %s: %w", role, err)
		}
		done = append(done, role)
	}
	return done, nil
}

// functions are the functions of schema ig each role may call. The
// persister adds the partition an old post or comment needs, through
// create_partition (migration 00014), which runs as the tables' owner.
var functions = map[string][]string{
	"igspersister": {"create_partition(text, text, integer, text[])"},
}

// RoleFunctions returns the functions of schema ig a role may call.
func RoleFunctions(role string) []string { return functions[role] }

// grants is what each role may do, table by table. It comes from the
// statements each component ran in the end-to-end and registry-sync tests
// (IGS_E2E_SQL_LOG, scripts/role-grants.py), and the tests run each
// component as its role, so a statement a role may not run fails CI.
var grants = map[string]map[string][]string{
	"igsapi": {
		"api_clients":           {"SELECT", "INSERT"},
		"comment_authors":       {"SELECT"},
		"comments":              {"SELECT"},
		"crawl_cursors":         {"SELECT"},
		"discovery_candidates":  {"SELECT", "INSERT", "UPDATE"},
		"discovery_keywords":    {"SELECT", "INSERT", "UPDATE", "DELETE"},
		"erasure_requests":      {"SELECT", "INSERT"},
		"highlights":            {"SELECT"},
		"jobs":                  {"SELECT", "INSERT", "UPDATE"},
		"media":                 {"SELECT"},
		"post_index":            {"SELECT"},
		"post_metrics":          {"SELECT"},
		"posts":                 {"SELECT"},
		"profile_registrations": {"SELECT", "INSERT", "DELETE"},
		"profile_schedule":      {"SELECT", "INSERT", "UPDATE", "DELETE"},
		"profile_snapshots":     {"SELECT"},
		"profiles":              {"SELECT"},
		"registry_sync_runs":    {"SELECT"},
	},
	"igscrawler": {
		"client_params":        {"SELECT"},
		"comments":             {"SELECT"},
		"crawl_cursors":        {"SELECT", "INSERT", "UPDATE"},
		"discovery_candidates": {"SELECT", "INSERT", "UPDATE"},
		"discovery_keywords":   {"SELECT"},
		"discovery_pages":      {"SELECT", "UPDATE"},
		"discovery_sources":    {"SELECT", "INSERT", "UPDATE"},
		"post_index":           {"SELECT"},
		"posts":                {"SELECT"},
		"profiles":             {"SELECT"},
		"queries":              {"SELECT"},
		"suppression":          {"SELECT"},
	},
	"igsmedia": {
		"media":      {"SELECT", "INSERT", "UPDATE"},
		"media_acks": {"SELECT", "INSERT", "UPDATE"},
	},
	"igspersister": {
		"comment_authors":   {"SELECT", "INSERT", "UPDATE"},
		"comment_edits":     {"SELECT", "INSERT"},
		"comment_metrics":   {"SELECT", "INSERT"},
		"comments":          {"SELECT", "INSERT", "UPDATE"},
		"highlights":        {"SELECT", "INSERT", "UPDATE"},
		"media":             {"SELECT", "INSERT", "UPDATE"},
		"post_edits":        {"SELECT", "INSERT"},
		"post_index":        {"SELECT", "INSERT"},
		"post_metrics":      {"SELECT", "INSERT"},
		"post_owners":       {"SELECT", "INSERT"},
		"posts":             {"SELECT", "INSERT", "UPDATE"},
		"profile_snapshots": {"SELECT", "INSERT"},
		"profiles":          {"SELECT", "INSERT", "UPDATE"},
		"suppression":       {"SELECT"},
	},
	"igsregistrysync": {
		"client_params":      {"SELECT"},
		"leases":             {"SELECT", "INSERT", "UPDATE", "DELETE"},
		"posts":              {"SELECT"},
		"profiles":           {"SELECT"},
		"queries":            {"SELECT", "INSERT", "UPDATE"},
		"registry_sync_runs": {"SELECT", "INSERT"},
	},
}
