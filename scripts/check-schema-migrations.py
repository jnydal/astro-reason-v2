#!/usr/bin/env python3
"""
CI guardrail: every Exposed table (Tables.kt, Provenance.kt) must have a migration
in infra/sql/ that creates it. Fails with non-zero exit if any code table is missing.

Usage: python scripts/check-schema-migrations.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def tables_from_kotlin() -> set[str]:
    """Extract table names from Exposed definitions in service/core."""
    tables = set()
    core = REPO_ROOT / "service" / "core" / "src" / "main" / "kotlin" / "com" / "astroreason" / "core"
    if not core.exists():
        return tables
    # Table("name") or UUIDTable("name", ...) or LongIdTable("name", ...)
    pattern = re.compile(r"(?:Table|UUIDTable|LongIdTable)\s*\(\s*\"(?P<name>[a-z0-9_]+)\"")
    for kt in core.rglob("*.kt"):
        text = kt.read_text(encoding="utf-8", errors="replace")
        for m in pattern.finditer(text):
            tables.add(m.group("name"))
    return tables


def tables_from_migrations() -> set[str]:
    """Extract table names from CREATE TABLE in infra/sql/*.sql."""
    tables = set()
    sql_dir = REPO_ROOT / "infra" / "sql"
    if not sql_dir.exists():
        return tables
    # CREATE TABLE [IF NOT EXISTS] name ( ...
    pattern = re.compile(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z0-9_]+)\s*[(]", re.IGNORECASE)
    for sql in sorted(sql_dir.glob("*.sql")):
        text = sql.read_text(encoding="utf-8", errors="replace")
        for m in pattern.finditer(text):
            tables.add(m.group(1).lower())
    return tables


def main() -> int:
    code_tables = tables_from_kotlin()
    migration_tables = tables_from_migrations()
    missing = code_tables - migration_tables
    if missing:
        print("Schema-migration invariant violated: the following Exposed tables have no CREATE TABLE in infra/sql/:", file=sys.stderr)
        for t in sorted(missing):
            print(f"  - {t}", file=sys.stderr)
        print("Add a migration in infra/sql/ for each new or modified table. See .cursor/rules/03-architecture-invariants.mdc", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
