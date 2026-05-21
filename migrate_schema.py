"""One-shot Phase 1 schema migration.

Reads the old per-context tables (global_user, swe_user, chalmers_user,
global_uni, swe_uni, global_country, swe_subdiv) and copies their rows into
the new unified obs tables with the appropriate `context` value.
`shortname` is left NULL for every migrated row.

Idempotent: aborts if any obs table already has rows.

Usage:
    python migrate_schema.py path/to/kattis.db
"""

import sqlite3
import sys

from db.kattis_db_conn import KattisDbConn

# (old_table, new_table, context, copy_sql_select_clause)
# Column order in the SELECT must match the INSERT target order below.
MIGRATIONS = [
    ('global_user',    'user_obs',    'global',
        'SELECT timestamp, ?, NULL, name, rank, score, place, uni FROM global_user'),
    ('swe_user',       'user_obs',    'swe',
        'SELECT timestamp, ?, NULL, name, rank, score, place, uni FROM swe_user'),
    ('chalmers_user',  'user_obs',    'chalmers',
        'SELECT timestamp, ?, NULL, name, rank, score, place, uni FROM chalmers_user'),

    ('global_uni',     'affiliation_obs', 'global',
        'SELECT timestamp, ?, NULL, uni, rank, score, subdiv, users FROM global_uni'),
    ('swe_uni',        'affiliation_obs', 'swe',
        'SELECT timestamp, ?, NULL, uni, rank, score, subdiv, users FROM swe_uni'),

    ('global_country', 'country_obs', 'global',
        'SELECT timestamp, ?, NULL, country, rank, score, users, unis FROM global_country'),

    ('swe_subdiv',     'subdivision_obs', 'swe',
        "SELECT timestamp, ?, NULL, subdiv, rank, score, 'SWE' FROM swe_subdiv"),
]

INSERT_TARGETS = {
    'user_obs':        '(timestamp, context, shortname, display_name, rank, score, place, affiliation)',
    'affiliation_obs': '(timestamp, context, shortname, display_name, rank, score, subdiv, num_users)',
    'country_obs':     '(timestamp, context, shortname, display_name, rank, score, num_users, num_affiliations)',
    'subdivision_obs': '(timestamp, context, shortname, display_name, rank, score, country)',
}


def existing_tables(conn):
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r[0] for r in rows}


def migrate(db_file):
    KattisDbConn(db_file)  # ensures the new tables exist
    conn = sqlite3.connect(db_file)

    tables = existing_tables(conn)
    for new in {m[1] for m in MIGRATIONS}:
        n = conn.execute(f'SELECT COUNT(*) FROM {new}').fetchone()[0]
        if n > 0:
            print(f'{new} already has {n} rows; migration appears to have already run. Aborting.', file=sys.stderr)
            sys.exit(2)

    total = 0
    for old, new, ctx, select_sql in MIGRATIONS:
        if old not in tables:
            print(f'  skip: {old} (not present)')
            continue
        before = conn.execute(f'SELECT COUNT(*) FROM {old}').fetchone()[0]
        conn.execute(f'INSERT INTO {new} {INSERT_TARGETS[new]} {select_sql}', (ctx,))
        after = conn.execute(f"SELECT COUNT(*) FROM {new} WHERE context=?", (ctx,)).fetchone()[0]
        assert before == after, f'{old} -> {new} ({ctx}): {before} src rows, {after} migrated'
        print(f'  {old:20s} -> {new:18s} context={ctx:10s} {after} rows')
        total += after

    conn.commit()
    print(f'\nMigrated {total} rows. Old tables left in place as backup.')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    migrate(sys.argv[1])
