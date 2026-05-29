import sqlite3
from collections import namedtuple
from datetime import datetime


# Named row returned by history(). Carries a superset of the per-type metrics;
# fields not applicable to a given entity kind are None. Lets the bot's plot
# loop select a metric by attribute name instead of a per-type tuple index.
HistoryRow = namedtuple(
    "HistoryRow",
    "timestamp rank display_name score num_users num_affiliations",
)


def _num(s, conv):
    # Kattis formats numbers with thousands-separator commas, e.g. "9,509.5".
    return conv(s.replace(',', ''))


# Maps the bot's user-facing type to the obs table that holds it.
_TABLE_BY_TYPE = {
    'user':    'user_obs',
    'uni':     'affiliation_obs',
    'country': 'country_obs',
}

# Columns selected for history(), per type. Only the fields the bot actually
# plots are projected; _HISTORY_ROW_BUILDER wraps each raw tuple into a
# HistoryRow, filling N/A metrics with None. (The unused place/affiliation/
# subdiv columns are intentionally not selected.)
_HISTORY_COLS_BY_TYPE = {
    'user':    'timestamp, rank, display_name, score',
    'uni':     'timestamp, rank, display_name, score, num_users',
    'country': 'timestamp, rank, display_name, score, num_users, num_affiliations',
}

_HISTORY_ROW_BUILDER = {
    'user':    lambda r: HistoryRow(r[0], r[1], r[2], r[3], None, None),
    'uni':     lambda r: HistoryRow(r[0], r[1], r[2], r[3], r[4], None),
    'country': lambda r: HistoryRow(r[0], r[1], r[2], r[3], r[4], r[5]),
}


def _allowed_contexts(type):
    a = ['global']
    if type in ('user', 'uni'): a.append('swe')
    if type == 'user':          a.append('chalmers')
    return a


class KattisDbConn:
    def __init__(self, db_file):
        # check_same_thread=False so the bot can do scraper.scrape_user from
        # asyncio.to_thread (worker thread). The SQLite file-level lock still
        # serializes writers across threads and processes.
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.create_tables()

    def create_tables(self):
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS user_obs ('
            'timestamp     INTEGER,'
            'context       TEXT,'
            'shortname     TEXT,'
            'display_name  TEXT,'
            'rank          INTEGER,'
            'score         REAL,'
            'place         TEXT,'
            'affiliation   TEXT'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS affiliation_obs ('
            'timestamp     INTEGER,'
            'context       TEXT,'
            'shortname     TEXT,'
            'display_name  TEXT,'
            'rank          INTEGER,'
            'score         REAL,'
            'subdiv        TEXT,'
            'num_users     INTEGER'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS country_obs ('
            'timestamp        INTEGER,'
            'context          TEXT,'
            'shortname        TEXT,'
            'display_name     TEXT,'
            'rank             INTEGER,'
            'score            REAL,'
            'num_users        INTEGER,'
            'num_affiliations INTEGER'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS subdivision_obs ('
            'timestamp     INTEGER,'
            'context       TEXT,'
            'shortname     TEXT,'
            'display_name  TEXT,'
            'rank          INTEGER,'
            'score         REAL,'
            'country       TEXT'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS language_obs ('
            'timestamp     INTEGER,'
            'context       TEXT,'
            'shortname     TEXT,'
            'display_name  TEXT,'
            'rank          INTEGER,'
            'score         REAL,'
            'num_users     INTEGER'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS entities ('
            'kind                  TEXT,'
            'shortname             TEXT,'
            'display_name          TEXT,'
            'tracked               INTEGER DEFAULT 0,'
            'discover_users        INTEGER DEFAULT 0,'
            'discover_affiliations INTEGER DEFAULT 0,'
            'first_seen            INTEGER,'
            'last_seen_alive       INTEGER,'
            'PRIMARY KEY (kind, shortname)'
            ')'
        )

        # ---- problem statistics (scraped by the problem_scraper service) ----
        # All time-series; no `context` column (problems aren't per-ranklist).
        # Inferable values are intentionally NOT stored (see CLAUDE.md):
        #   submission_ratio = accepted / submissions
        #   full_solve_ratio = full_solves / authors
        #   difficulty_category derives from the difficulty number.
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS problem_obs ('
            'timestamp        INTEGER,'
            'shortname        TEXT,'
            'display_name     TEXT,'
            'difficulty_low   REAL,'
            'difficulty_high  REAL,'
            'submissions      INTEGER,'
            'accepted         INTEGER,'
            'authors          INTEGER,'
            'full_solves      INTEGER'
            ')'
        )
        # Verdict donut: one row per slice, so arbitrary verdict labels
        # (Memory Limit Exceeded, Judge Error, ...) are all captured.
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS problem_verdict ('
            'timestamp     INTEGER,'
            'shortname     TEXT,'
            'verdict       TEXT,'
            'count         INTEGER'
            ')'
        )
        # Partial-score difficulty breakpoints: one row per breakpoint.
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS problem_partial_difficulty ('
            'timestamp     INTEGER,'
            'shortname     TEXT,'
            'breakpoint    REAL,'
            'difficulty    REAL'
            ')'
        )
        # All-languages toplists. kind in {best_scoring, fastest, shortest}.
        # value = score / runtime-seconds / byte-length depending on kind.
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS problem_toplist ('
            'timestamp         INTEGER,'
            'shortname         TEXT,'
            'kind              TEXT,'
            'rank              INTEGER,'
            'user_shortname    TEXT,'
            'user_display_name TEXT,'
            'value             REAL,'
            'language          TEXT,'
            'solved_at         TEXT'
            ')'
        )
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_problem_obs_sn_ts ON problem_obs (shortname, timestamp)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_problem_toplist_sn_ts ON problem_toplist (shortname, timestamp)')
        # Speeds the bot's history() `display_name IN (...)` reads and the
        # per-keystroke autocomplete `LIKE` scans (distinct_display_names).
        for t in ('user_obs', 'affiliation_obs', 'country_obs'):
            self.conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{t}_dn ON {t} (display_name)')

    # ---- write path ----------------------------------------------------------
    #
    # Row cells are (text, slug) tuples as returned by Scraper.parse_cell.
    # `slug` is the path of the first <a href> in the cell with the leading
    # slash stripped (e.g. 'users/joshua-andersson', 'countries/SWE/O'), or
    # None if the cell had no anchor. Each helper strips the prefix it
    # expects and stores just the entity identifier in `shortname`.

    def add_user_rows(self, rows, context, timestamp, force_tracked=False):
        a = []
        for r in rows:
            name_text, name_slug = r[1]
            sn = _strip(name_slug, 'users/')
            a.append((
                timestamp, context, sn,
                name_text,                  # display_name
                _num(r[0][0], int),         # rank
                _num(r[4][0], float),       # score
                r[2][0],                    # place
                r[3][0],                    # affiliation
            ))
            self._touch_entity('user', sn, name_text, timestamp, qualifies_tracked=(context == 'global' or force_tracked))
        self.conn.executemany(
            'INSERT INTO user_obs (timestamp, context, shortname, display_name, rank, score, place, affiliation) '
            'VALUES (?,?,?,?,?,?,?,?)', a
        )
        self.conn.commit()

    def add_affiliation_rows(self, rows, context, timestamp, force_tracked=False):
        a = []
        for r in rows:
            name_text, name_slug = r[1]
            sn = _strip(name_slug, 'affiliations/')
            a.append((
                timestamp, context, sn,
                name_text,                  # display_name
                _num(r[0][0], int),         # rank
                _num(r[4][0], float),       # score
                r[2][0],                    # subdiv
                _num(r[3][0], int),         # num_users
            ))
            self._touch_entity('affiliation', sn, name_text, timestamp, qualifies_tracked=(context == 'global' or force_tracked))
        self.conn.executemany(
            'INSERT INTO affiliation_obs (timestamp, context, shortname, display_name, rank, score, subdiv, num_users) '
            'VALUES (?,?,?,?,?,?,?,?)', a
        )
        self.conn.commit()

    def add_country_rows(self, rows, context, timestamp, force_tracked=False):
        a = []
        for r in rows:
            name_text, name_slug = r[1]
            sn = _strip(name_slug, 'countries/')
            a.append((
                timestamp, context, sn,
                name_text,                  # display_name
                _num(r[0][0], int),         # rank
                _num(r[4][0], float),       # score
                _num(r[2][0], int),         # num_users
                _num(r[3][0], int),         # num_affiliations
            ))
            self._touch_entity('country', sn, name_text, timestamp, qualifies_tracked=(context == 'global' or force_tracked))
        self.conn.executemany(
            'INSERT INTO country_obs (timestamp, context, shortname, display_name, rank, score, num_users, num_affiliations) '
            'VALUES (?,?,?,?,?,?,?,?)', a
        )
        self.conn.commit()

    def add_subdivision_rows(self, rows, context, timestamp, country, force_tracked=False):
        a = []
        for r in rows:
            name_text, name_slug = r[1]
            sn = _strip(name_slug, 'countries/')
            a.append((
                timestamp, context, sn,
                name_text,                  # display_name
                _num(r[0][0], int),         # rank
                _num(r[2][0], float),       # score
                country,
            ))
            self._touch_entity('subdivision', sn, name_text, timestamp, qualifies_tracked=(context == 'global' or force_tracked))
        self.conn.executemany(
            'INSERT INTO subdivision_obs (timestamp, context, shortname, display_name, rank, score, country) '
            'VALUES (?,?,?,?,?,?,?)', a
        )
        self.conn.commit()

    def add_language_rows(self, rows, context, timestamp, force_tracked=False):
        a = []
        for r in rows:
            name_text, _ = r[1]
            # Languages have no anchor on the ranklist page — use the display
            # name as the canonical shortname so the entities PK stays unique.
            sn = name_text
            a.append((
                timestamp, context, sn,
                name_text,                  # display_name
                _num(r[0][0], int),         # rank
                _num(r[3][0], float),       # score
                _num(r[2][0], int),         # num_users
            ))
            self._touch_entity('language', sn, name_text, timestamp, qualifies_tracked=(context == 'global' or force_tracked))
        self.conn.executemany(
            'INSERT INTO language_obs (timestamp, context, shortname, display_name, rank, score, num_users) '
            'VALUES (?,?,?,?,?,?,?)', a
        )
        self.conn.commit()

    def add_user_backstop(self, shortname, display_name, rank, score, timestamp):
        """Single-row write for the per-user backstop (/users/<slug>)."""
        self.conn.execute(
            'INSERT INTO user_obs '
            '  (timestamp, context, shortname, display_name, rank, score, place, affiliation) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (timestamp, 'global', shortname, display_name, rank, score, None, None)
        )
        # Touch entity — qualifies_tracked=True because backstop only runs for
        # already-tracked users and the observation alone proves liveness.
        self._touch_entity('user', shortname, display_name, timestamp, qualifies_tracked=True)
        self.conn.commit()

    # ---- problem write path --------------------------------------------------

    def register_problems(self, rows, timestamp):
        """Discovery: register problems found on the /problems listing as
        tracked entities. `rows` is an iterable of (shortname, display_name)."""
        for shortname, display_name in rows:
            self._touch_entity('problem', shortname, display_name, timestamp, qualifies_tracked=True)
        self.conn.commit()

    def add_problem_obs(self, shortname, display_name, timestamp,
                        difficulty_low, difficulty_high,
                        submissions, accepted, authors, full_solves):
        self.conn.execute(
            'INSERT INTO problem_obs '
            '  (timestamp, shortname, display_name, difficulty_low, difficulty_high, '
            '   submissions, accepted, authors, full_solves) '
            'VALUES (?,?,?,?,?,?,?,?,?)',
            (timestamp, shortname, display_name, difficulty_low, difficulty_high,
             submissions, accepted, authors, full_solves)
        )
        self._touch_entity('problem', shortname, display_name, timestamp, qualifies_tracked=True)
        self.conn.commit()

    def add_problem_verdicts(self, shortname, timestamp, slices):
        """`slices` is an iterable of (verdict, count)."""
        self.conn.executemany(
            'INSERT INTO problem_verdict (timestamp, shortname, verdict, count) VALUES (?,?,?,?)',
            [(timestamp, shortname, v, c) for v, c in slices]
        )
        self.conn.commit()

    def add_problem_partial_difficulty(self, shortname, timestamp, breakpoints):
        """`breakpoints` is an iterable of (breakpoint, difficulty)."""
        self.conn.executemany(
            'INSERT INTO problem_partial_difficulty (timestamp, shortname, breakpoint, difficulty) VALUES (?,?,?,?)',
            [(timestamp, shortname, bp, d) for bp, d in breakpoints]
        )
        self.conn.commit()

    def add_problem_toplist(self, shortname, timestamp, kind, rows):
        """`rows` is an iterable of
        (rank, user_shortname, user_display_name, value, language, solved_at)."""
        self.conn.executemany(
            'INSERT INTO problem_toplist '
            '  (timestamp, shortname, kind, rank, user_shortname, user_display_name, value, language, solved_at) '
            'VALUES (?,?,?,?,?,?,?,?,?)',
            [(timestamp, shortname, kind, *r) for r in rows]
        )
        self.conn.commit()

    def problems_to_scrape(self, alive_since):
        """All tracked, alive problems ordered stalest-first (never-scraped
        first, then oldest problem_obs). Returns [(shortname, display_name)].
        The problem_scraper snapshots this once per rotation and iterates it."""
        return self.conn.execute('''
            SELECT e.shortname, e.display_name
            FROM entities e
            LEFT JOIN (SELECT shortname, MAX(timestamp) mt FROM problem_obs GROUP BY shortname) p
              ON p.shortname = e.shortname
            WHERE e.kind='problem' AND e.tracked=1
              AND (e.last_seen_alive IS NULL OR e.last_seen_alive > ?)
            ORDER BY COALESCE(p.mt, 0) ASC
        ''', (alive_since,)).fetchall()

    def _touch_entity(self, kind, shortname, display_name, timestamp, qualifies_tracked):
        # `shortname` may legitimately be None if the source page had no
        # anchor for this entity. Fall back to display_name so the PK is
        # always populated (SQLite treats multiple NULLs as distinct, which
        # would defeat uniqueness).
        if shortname is None:
            shortname = display_name
        self.conn.execute(
            'INSERT INTO entities '
            '  (kind, shortname, display_name, tracked, first_seen, last_seen_alive) '
            'VALUES (?, ?, ?, ?, ?, ?) '
            'ON CONFLICT(kind, shortname) DO UPDATE SET '
            '  display_name=excluded.display_name, '
            '  tracked=max(tracked, excluded.tracked), '
            '  last_seen_alive=max(last_seen_alive, excluded.last_seen_alive)',
            (kind, shortname, display_name, int(qualifies_tracked), timestamp, timestamp)
        )

    def set_flags(self, kind, shortname, **flags):
        """Manually set entity flags. Creates the row if absent.
        Allowed flags: tracked, discover_users, discover_affiliations."""
        allowed = {'tracked', 'discover_users', 'discover_affiliations'}
        bad = set(flags) - allowed
        if bad:
            raise ValueError(f'unsupported flags: {sorted(bad)}')
        cols = ', '.join(flags.keys())
        vals = list(flags.values())
        placeholders = ', '.join('?' * len(vals))
        updates = ', '.join(f'{k}=excluded.{k}' for k in flags)
        self.conn.execute(
            f'INSERT INTO entities (kind, shortname, {cols}) '
            f'VALUES (?, ?, {placeholders}) '
            f'ON CONFLICT(kind, shortname) DO UPDATE SET {updates}',
            (kind, shortname, *vals)
        )
        self.conn.commit()

    # ---- read path -----------------------------------------------------------

    def max_time(self, timestamp=False):
        t = self.conn.execute('SELECT MAX(timestamp) FROM affiliation_obs').fetchone()[0]
        if t is None: return t
        if timestamp: return t
        return datetime.fromtimestamp(t)

    def history(self, mintimestamp, type, names, place='all'):
        allowed = _allowed_contexts(type)
        if place == 'all':
            contexts = allowed
        elif place in allowed:
            contexts = [place]
        else:
            return [(name, []) for name in names]

        if not names:
            return []

        cols = _HISTORY_COLS_BY_TYPE[type]
        table = _TABLE_BY_TYPE[type]
        build = _HISTORY_ROW_BUILDER[type]
        qs_names = ','.join('?' * len(names))
        qs_ctx = ','.join('?' * len(contexts))
        raw = self.conn.execute(
            f'SELECT {cols} FROM {table} '
            f'WHERE display_name IN ({qs_names}) '
            f'AND context IN ({qs_ctx}) '
            f'AND timestamp >= ?',
            (*names, *contexts, mintimestamp)
        ).fetchall()
        rows = [build(x) for x in raw]

        # Sort by timestamp explicitly: HistoryRow sorts lexicographically and a
        # None metric vs a number would TypeError on later fields.
        r = [(name, sorted((row for row in rows if row.display_name == name),
                           key=lambda row: row.timestamp))
             for name in names]
        if place == 'all':
            # Same entity at the same scrape appears once per context (chalmers/swe/global).
            # Score is identical across contexts; collapse near-simultaneous duplicates.
            for _, x in r:
                i = 1
                while i < len(x):
                    if x[i].timestamp - x[i-1].timestamp < 3600:
                        x.pop(i)
                    else:
                        i += 1
        return r

    def distinct_display_names(self, type, prefix, limit=25):
        """Autocomplete helper: distinct display names for an entity type whose
        name starts with `prefix` (case-insensitive). Empty prefix returns an
        arbitrary `limit` of them. `limit` enforces Discord's 25-choice cap."""
        table = _TABLE_BY_TYPE.get(type)
        if table is None:
            return []
        if prefix:
            # Build the pattern in Python (bound param) and escape LIKE
            # wildcards in the user-typed prefix. Avoids relying on SQLite's
            # double-quoted-string fallback for the '%'. LIKE is already
            # ASCII case-insensitive, so no COLLATE is needed.
            esc = prefix.replace('\\', r'\\').replace('%', r'\%').replace('_', r'\_')
            rows = self.conn.execute(
                f"SELECT DISTINCT display_name FROM {table} "
                f"WHERE display_name LIKE ? ESCAPE '\\' LIMIT ?",
                (esc + '%', limit)
            ).fetchall()
        else:
            rows = self.conn.execute(
                f'SELECT DISTINCT display_name FROM {table} LIMIT ?', (limit,)
            ).fetchall()
        return [x[0] for x in rows]

    def get_top(self, type, place, cnt):
        table = _TABLE_BY_TYPE.get(type)
        if table is None or place not in _allowed_contexts(type):
            return None
        # Take each entity's LATEST row in this context, then sort by score.
        # We can't just take MAX(timestamp)'s rows: for users the per-user
        # backstop writes lone context='global' rows at their own timestamps,
        # so the single latest timestamp is often one backstopped user — which
        # would make top-N return just that one. Per-entity-latest is robust to
        # that (and identical to the old behaviour for backstop-free types).
        rows = self.conn.execute(
            f'SELECT u.display_name, u.score FROM {table} u '
            f'JOIN (SELECT display_name, MAX(timestamp) AS mt FROM {table} '
            f'      WHERE context=? GROUP BY display_name) m '
            f'  ON m.display_name=u.display_name AND m.mt=u.timestamp '
            f'WHERE u.context=? '
            f'ORDER BY u.score DESC LIMIT ?',
            (place, place, cnt)
        ).fetchall()
        return [x[0] for x in rows]

    def printall(self):
        for table in ['user_obs', 'affiliation_obs', 'country_obs', 'subdivision_obs', 'language_obs', 'entities']:
            print('----------', table, '----------')
            x = self.conn.execute('SELECT * from ' + table).fetchall()
            print('\n'.join(str(y) for y in x))
            print()


def _strip(slug, prefix):
    if slug is None: return None
    return slug.removeprefix(prefix)
