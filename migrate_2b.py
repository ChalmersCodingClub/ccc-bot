"""Phase 2b schema + seed migration.

Renames `entities.ever_top_100` -> `entities.tracked` and seeds the rows
that were previously implicit (the hardcoded scrape_swe / scrape_chalmers
jobs in Phase 2a) so the dynamic job builder picks them up.

Idempotent: detects when the rename has already been applied and skips it.

Usage:
    python migrate_2b.py path/to/kattis.db
"""

import sqlite3
import sys


def _has_column(conn, table, col):
    rows = conn.execute(f'PRAGMA table_info({table})').fetchall()
    return any(r[1] == col for r in rows)


def migrate(db_file):
    conn = sqlite3.connect(db_file)

    if not _has_column(conn, 'entities', 'tracked'):
        if not _has_column(conn, 'entities', 'ever_top_100'):
            print('entities table missing both `tracked` and `ever_top_100`; '
                  'expected Phase 2a schema. Aborting.', file=sys.stderr)
            sys.exit(2)
        print('renaming entities.ever_top_100 -> entities.tracked')
        conn.execute('ALTER TABLE entities RENAME COLUMN ever_top_100 TO tracked')
        conn.commit()
    else:
        print('entities.tracked already present; skipping rename')

    # Seed the implicit Phase-2a discovery targets so the dynamic job builder
    # produces a country/SWE and affiliation/chalmers.se job.
    seeds = [
        ('country',     'SWE',         'Sweden',                              1, 1, 1),
        ('affiliation', 'chalmers.se', 'Chalmers University of Technology',   1, 1, 0),
    ]
    for kind, sn, dn, tr, du, da in seeds:
        existing = conn.execute(
            'SELECT 1 FROM entities WHERE kind=? AND shortname=?', (kind, sn)
        ).fetchone()
        if existing:
            conn.execute(
                'UPDATE entities SET tracked=max(tracked, ?), '
                'discover_users=max(discover_users, ?), '
                'discover_affiliations=max(discover_affiliations, ?) '
                'WHERE kind=? AND shortname=?',
                (tr, du, da, kind, sn)
            )
            print(f'  updated flags on {kind}/{sn}')
        else:
            now = conn.execute("SELECT strftime('%s','now')").fetchone()[0]
            conn.execute(
                'INSERT INTO entities (kind, shortname, display_name, tracked, '
                'discover_users, discover_affiliations, first_seen, last_seen_alive) '
                'VALUES (?,?,?,?,?,?,?,?)',
                (kind, sn, dn, tr, du, da, now, now)
            )
            print(f'  inserted {kind}/{sn}')
    conn.commit()
    print('done.')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    migrate(sys.argv[1])
