import sqlite3
from datetime import datetime


def _num(s, conv):
    # Kattis formats numbers with thousands-separator commas, e.g. "9,509.5".
    return conv(s.replace(',', ''))


# Maps the bot's user-facing type to the obs table that holds it.
_TABLE_BY_TYPE = {
    'user':    'user_obs',
    'uni':     'affiliation_obs',
    'country': 'country_obs',
}

# Columns selected for history(), in the order main.py unpacks them.
# Kept identical to the pre-migration row shape so main.py needs no change.
_HISTORY_COLS_BY_TYPE = {
    'user':    'timestamp, rank, display_name, place, affiliation, score',
    'uni':     'timestamp, rank, display_name, subdiv, num_users, score',
    'country': 'timestamp, rank, display_name, num_users, num_affiliations, score',
}


def _allowed_contexts(type):
    a = ['global']
    if type in ('user', 'uni'): a.append('swe')
    if type == 'user':          a.append('chalmers')
    return a


class KattisDbConn:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
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
        qs_names = ','.join('?' * len(names))
        qs_ctx = ','.join('?' * len(contexts))
        rows = self.conn.execute(
            f'SELECT {cols} FROM {table} '
            f'WHERE display_name IN ({qs_names}) '
            f'AND context IN ({qs_ctx}) '
            f'AND timestamp >= ?',
            (*names, *contexts, mintimestamp)
        ).fetchall()

        r = [(name, sorted(row for row in rows if row[2] == name)) for name in names]
        if place == 'all':
            # Same entity at the same scrape appears once per context (chalmers/swe/global).
            # Score is identical across contexts; collapse near-simultaneous duplicates.
            for _, x in r:
                i = 1
                while i < len(x):
                    if x[i][0] - x[i-1][0] < 3600:
                        x.pop(i)
                    else:
                        i += 1
        return r

    def get_top(self, type, place, cnt):
        table = _TABLE_BY_TYPE.get(type)
        if table is None or place not in _allowed_contexts(type):
            return None
        # Per-URL scraping decoupled the per-table timestamps, so each query
        # must pick its OWN table+context's latest, not a global max_time.
        t = self.conn.execute(
            f'SELECT MAX(timestamp) FROM {table} WHERE context=?', (place,)
        ).fetchone()[0]
        if t is None:
            return []
        rows = self.conn.execute(
            f'SELECT display_name, score FROM {table} WHERE timestamp=? AND context=?',
            (t, place)
        ).fetchall()
        rows.sort(key=lambda x: -x[1])
        return [x[0] for x in rows[:cnt]]

    def printall(self):
        for table in ['user_obs', 'affiliation_obs', 'country_obs', 'subdivision_obs', 'language_obs', 'entities']:
            print('----------', table, '----------')
            x = self.conn.execute('SELECT * from ' + table).fetchall()
            print('\n'.join(str(y) for y in x))
            print()


def _strip(slug, prefix):
    if slug is None: return None
    return slug.removeprefix(prefix)
