#!/usr/bin/env python3
"""Derives each component's PostgreSQL grants from the statements it ran.

The end-to-end tests record every statement each component sends when
IGS_E2E_SQL_LOG names a file ("component<TAB>statement" lines). This script
reads such files and prints, per component, the privileges each table needs,
as the Go map in internal/store/roles.go:

    IGS_E2E_SQL_LOG=/tmp/sql.log go test ./internal/ig/e2e/ ./internal/ig/registrysync/
    python3 scripts/role-grants.py /tmp/sql.log

Rules:
- SELECT for every table read (FROM, JOIN), and for every table written: an
  UPDATE or DELETE reads its WHERE clause, RETURNING reads the row.
- INSERT for INSERT INTO and COPY; UPDATE for UPDATE and for INSERT ... ON
  CONFLICT ... DO UPDATE; DELETE for DELETE FROM.
- SELECT ... FOR UPDATE, FOR NO KEY UPDATE or FOR SHARE needs UPDATE on the
  tables it locks.
Only igs's own tables count (from migrations/ig): CTE names, functions and
other schemas are left out.
"""
import glob
import os
import re
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The components that have a role of their own; the others keep the owner.
COMPONENTS = {"api": "igsapi", "crawler": "igscrawler", "persister": "igspersister",
              "media": "igsmedia", "registry-sync": "igsregistrysync"}


# Privileges the tests don't exercise but the component's own code needs,
# each checked by hand against the code (role, table, privileges, where).
EXTRA = [
    ("igsapi", "api_clients", {"SELECT", "INSERT"}, "igs-api client create (cmd/igs-api), with the API's credentials"),
    ("igsapi", "jobs", {"SELECT", "UPDATE"}, "dispatch marks a job failed when its tasks can't be produced"),
    ("igsapi", "highlights", {"SELECT"}, "GET /v1/profiles/{profile}/highlights (internal/ig/api/data.go)"),
    ("igsapi", "profile_snapshots", {"SELECT"}, "GET /v1/profiles/{profile}/snapshots (internal/ig/api/data.go)"),
    ("igsapi", "registry_sync_runs", {"SELECT"}, "GET /v1/admin/registry-sync/runs (internal/ig/api/admin.go)"),
    ("igscrawler", "profiles", {"SELECT"}, "the profile stage looks up a task's username by user pk"),
    ("igsregistrysync", "posts", {"SELECT"}, "a canary page's stored posts, to check the comment queries (storedPosts)"),
]


def tables():
    out = set()
    for f in glob.glob(os.path.join(ROOT, "migrations/ig/*.sql")):
        with open(f) as fh:
            out |= {m.lower() for m in re.findall(r"CREATE TABLE (?:IF NOT EXISTS )?(?:ig\.)?([a-z_]+)", fh.read(), re.I)}
    return out


def privileges(sql, known):
    """{table: {privilege}} for one statement."""
    s = re.sub(r"'(?:[^']|'')*'", "''", sql)  # drop string literals
    out = defaultdict(set)

    def add(name, priv):
        name = name.lower().split(".")[-1].strip('"')
        if name in known:
            out[name].add(priv)

    for m in re.finditer(r"\bINSERT\s+INTO\s+([\w.\"]+)", s, re.I):
        add(m.group(1), "INSERT")
        if re.search(r"\bON\s+CONFLICT\b[^;]*\bDO\s+UPDATE\b", s, re.I):
            add(m.group(1), "UPDATE")
        add(m.group(1), "SELECT")
    for m in re.finditer(r"^COPY\s+([\w.\"]+)", s, re.I):
        add(m.group(1), "INSERT")
    for m in re.finditer(r"\bUPDATE\s+([\w.\"]+)\s+(?:AS\s+\w+\s+)?SET\b", s, re.I):
        add(m.group(1), "UPDATE")
        add(m.group(1), "SELECT")
    for m in re.finditer(r"\bDELETE\s+FROM\s+([\w.\"]+)", s, re.I):
        add(m.group(1), "DELETE")
        add(m.group(1), "SELECT")
    locks = re.search(r"\bFOR\s+(?:NO\s+KEY\s+)?(?:UPDATE|SHARE)\b", s, re.I)
    for m in re.finditer(r"\b(?:FROM|JOIN)\s+([\w.\"]+)", s, re.I):
        if re.match(r"(?i)DELETE\s+FROM", s[max(0, m.start() - 7):m.end()]):
            continue
        add(m.group(1), "SELECT")
        if locks:
            add(m.group(1), "UPDATE")
    return out


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: role-grants.py sql.log [more.log ...]")
    known = tables()
    grants = defaultdict(lambda: defaultdict(set))
    for path in sys.argv[1:]:
        with open(path) as fh:
            for line in fh:
                comp, _, sql = line.rstrip("\n").partition("\t")
                if comp not in COMPONENTS:
                    continue
                for t, privs in privileges(sql, known).items():
                    grants[COMPONENTS[comp]][t] |= privs
    for role, table, privs, _ in EXTRA:
        grants[role][table] |= privs
    order = ["SELECT", "INSERT", "UPDATE", "DELETE"]
    print("var grants = map[string]map[string][]string{")
    for role in sorted(grants):
        print(f"\t{role!r}: {{".replace("'", '"'))
        for t in sorted(grants[role]):
            privs = ", ".join(f'"{p}"' for p in order if p in grants[role][t])
            print(f'\t\t"{t}": {{{privs}}},')
        print("\t},")
    print("}")


if __name__ == "__main__":
    main()
