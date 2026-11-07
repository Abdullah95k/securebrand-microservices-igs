#!/usr/bin/env python3
"""Checks that a change keeps the previous release working on the new schema
(REQ-008 fix 23, gap A6).

The migrate hook upgrades the schema before the new pods start, and the old
pods keep running on it until they are replaced (and after a rollback). So,
against the base revision:

- a migration already on the base is never edited, renamed or removed
  (goose has already run it in production: an edit would never apply);
- a new migration comes after every existing one in its directory;
- a new migration's Up section only adds: no DROP TABLE or COLUMN, no
  RENAME, no change of a column's type, no SET NOT NULL, no NOT NULL column
  without a DEFAULT, no TRUNCATE, no MODIFY COLUMN.

A migration that has to break this says so in a comment, with the reason and
the release plan: `-- igs:breaking: <reason>`. It is then reported, not
refused.

    python3 scripts/migration-check.py --base origin/main
    python3 scripts/migration-check.py --self-test
"""
import argparse
import os
import re
import subprocess
import sys

DIRS = ("migrations/ig",)
MARKER = re.compile(r"--\s*igs:breaking:\s*(\S.*)", re.I)
RULES = [
    (re.compile(r"\bDROP\s+(TABLE|COLUMN|VIEW|SCHEMA|MATERIALIZED\s+VIEW)\b", re.I), "drops a table, view or column"),
    (re.compile(r"\bRENAME\b", re.I), "renames something"),
    (re.compile(r"\bALTER\s+COLUMN\s+\S+\s+(SET\s+DATA\s+)?TYPE\b", re.I), "changes a column's type"),
    (re.compile(r"\bALTER\s+COLUMN\s+\S+\s+SET\s+NOT\s+NULL\b", re.I), "makes a column NOT NULL"),
    (re.compile(r"\bMODIFY\s+COLUMN\b", re.I), "modifies a column"),
    (re.compile(r"\bTRUNCATE\b", re.I), "empties a table"),
]
ADD_NOT_NULL = re.compile(r"\bADD\s+COLUMN\b[^,;]*\bNOT\s+NULL\b", re.I)


def up_statements(sql):
    """The Up section's statements, comments removed (goose files; a file
    without goose markers is all Up)."""
    if "+goose Up" in sql:
        sql = sql.split("+goose Up", 1)[1]
    if "+goose Down" in sql:
        sql = sql.split("+goose Down", 1)[0]
    lines = [re.sub(r"--.*", "", ln) for ln in sql.splitlines()]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def problems(sql):
    """What a new migration does that the previous release may not survive."""
    out = []
    for st in up_statements(sql):
        flat = " ".join(st.split())
        for rx, what in RULES:
            if rx.search(flat):
                out.append(f"{what}: {flat[:120]}")
        for part in re.split(r",\s*(?=ADD\s+COLUMN)", flat, flags=re.I):
            if ADD_NOT_NULL.search(part) and not re.search(r"\bDEFAULT\b", part, re.I):
                out.append(f"adds a NOT NULL column without a DEFAULT: {part[:120]}")
    return out


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def files_at(rev, d):
    """{name: blob id} of the .sql files in d at rev."""
    out = {}
    for line in git("ls-tree", rev, d + "/").splitlines():
        meta, path = line.split("\t", 1)
        if path.endswith(".sql"):
            out[os.path.basename(path)] = meta.split()[2]
    return out


def number(name):
    m = re.match(r"(\d+)_", name)
    return int(m.group(1)) if m else -1


def check(base, head="HEAD"):
    errs, notes = [], []
    for d in DIRS:
        old, new = files_at(base, d), files_at(head, d)
        for name, blob in sorted(old.items()):
            if name not in new:
                errs.append(f"{d}/{name}: a migration already on {base} was removed or renamed; add a new one instead")
            elif new[name] != blob:
                errs.append(f"{d}/{name}: a migration already on {base} was edited; it has run already, so add a new one instead")
        top = max((number(n) for n in old), default=0)
        for name in sorted(set(new) - set(old)):
            path = f"{d}/{name}"
            if number(name) <= top:
                errs.append(f"{path}: numbered {number(name)}, not after the existing {top}: goose would not run it in order")
            sql = git("show", f"{head}:{path}")
            found = problems(sql)
            m = MARKER.search(sql)
            if found and m:
                notes.append(f"{path}: breaking on purpose ({m.group(1).strip()}): " + "; ".join(found))
            elif found:
                errs.extend(f"{path}: {p}" for p in found)
    return errs, notes


def self_test():
    cases = {
        "ALTER TABLE profiles ADD COLUMN external_url text;": [],
        "ALTER TABLE posts ADD COLUMN kind text NOT NULL DEFAULT 'post';": [],
        "CREATE TABLE t (a int NOT NULL, b text NOT NULL);": [],
        "CREATE INDEX x ON t (a) WHERE a IS NOT NULL;": [],
        "ALTER TABLE t ALTER COLUMN tier SET DEFAULT 'long_tail';": [],
        "ALTER TABLE t ADD COLUMN n int NOT NULL;": ["adds a NOT NULL column without a DEFAULT"],
        "ALTER TABLE t ADD COLUMN a int, ADD COLUMN n int NOT NULL;": ["adds a NOT NULL column without a DEFAULT"],
        "DROP TABLE highlight_items;": ["drops"],
        "ALTER TABLE t DROP COLUMN a;": ["drops"],
        "ALTER TABLE t RENAME COLUMN a TO b;": ["renames"],
        "ALTER TABLE t ALTER COLUMN a TYPE bigint;": ["changes a column's type"],
        "ALTER TABLE t ALTER COLUMN a SET NOT NULL;": ["makes a column NOT NULL"],
        "ALTER TABLE t MODIFY COLUMN a UInt64;": ["modifies a column"],
        "-- +goose Up\nALTER TABLE t ADD COLUMN a text;\n-- +goose Down\nALTER TABLE t DROP COLUMN a;": [],
        "-- DROP TABLE in a comment only\nALTER TABLE t ADD COLUMN a text;": [],
    }
    bad = 0
    for sql, want in cases.items():
        got = problems(sql)
        ok = len(got) == len(want) and all(w in g for w, g in zip(want, got))
        if not ok:
            bad += 1
            print(f"self-test: {sql!r}: got {got}, want {want}", file=sys.stderr)
    if MARKER.search("-- igs:breaking: drop the old table after release 1.4") is None:
        bad += 1
        print("self-test: the marker is not recognised", file=sys.stderr)
    return bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="origin/main")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        bad = self_test()
        print("migration check self-test:", "FAIL" if bad else "ok")
        sys.exit(1 if bad else 0)
    errs, notes = check(a.base)
    for n in notes:
        print("note:", n)
    for e in errs:
        print(e, file=sys.stderr)
    if errs:
        print("The previous release must keep running on the new schema (docs/runbooks/igs-operations.md, "
              "\"Schema changes\").", file=sys.stderr)
        sys.exit(1)
    print(f"migration check: against {a.base}, merged migrations unchanged and new ones only add")


if __name__ == "__main__":
    main()
