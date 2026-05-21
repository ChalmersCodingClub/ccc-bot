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
            'ever_top_100          INTEGER DEFAULT 0,'
            'discover_users        INTEGER DEFAULT 0,'
            'discover_affiliations INTEGER DEFAULT 0,'
            'first_seen            INTEGER,'
            'last_seen_alive       INTEGER,'
            'PRIMARY KEY (kind, shortname)'
            ')'
        )

    # ---- write path (Phase 1: same data, new tables) -------------------------

    def _add_user_rows(self, rows, context, timestamp):
        a = [(
            timestamp, context, None,
            r[1],                       # display_name
            _num(r[0], int),            # rank
            _num(r[4], float),          # score
            r[2],                       # place (country or subdiv or both)
            r[3],                       # affiliation
        ) for r in rows]
        self.conn.executemany(
            'INSERT INTO user_obs (timestamp, context, shortname, display_name, rank, score, place, affiliation) '
            'VALUES (?,?,?,?,?,?,?,?)', a
        )

    def _add_affiliation_rows(self, rows, context, timestamp):
        a = [(
            timestamp, context, None,
            r[1],                       # display_name
            _num(r[0], int),            # rank
            _num(r[4], float),          # score
            r[2],                       # subdiv
            _num(r[3], int),            # num_users
        ) for r in rows]
        self.conn.executemany(
            'INSERT INTO affiliation_obs (timestamp, context, shortname, display_name, rank, score, subdiv, num_users) '
            'VALUES (?,?,?,?,?,?,?,?)', a
        )

    def _add_country_rows(self, rows, context, timestamp):
        a = [(
            timestamp, context, None,
            r[1],                       # display_name
            _num(r[0], int),            # rank
            _num(r[4], float),          # score
            _num(r[2], int),            # num_users
            _num(r[3], int),            # num_affiliations
        ) for r in rows]
        self.conn.executemany(
            'INSERT INTO country_obs (timestamp, context, shortname, display_name, rank, score, num_users, num_affiliations) '
            'VALUES (?,?,?,?,?,?,?,?)', a
        )

    def _add_subdivision_rows(self, rows, context, timestamp, country):
        a = [(
            timestamp, context, None,
            r[1],                       # display_name
            _num(r[0], int),            # rank
            _num(r[2], float),          # score
            country,
        ) for r in rows]
        self.conn.executemany(
            'INSERT INTO subdivision_obs (timestamp, context, shortname, display_name, rank, score, country) '
            'VALUES (?,?,?,?,?,?,?)', a
        )

    def add_data(self, global_uni, global_user, global_country, swe_tables, chalmers_user, time=None):
        if time is None: time = datetime.now()
        time = int(time.timestamp())

        self._add_affiliation_rows(global_uni,     'global', time)
        self._add_user_rows(       global_user,    'global', time)
        self._add_country_rows(    global_country, 'global', time)

        self._add_affiliation_rows( swe_tables[0], 'swe', time)
        self._add_user_rows(        swe_tables[1], 'swe', time)
        self._add_subdivision_rows( swe_tables[2], 'swe', time, country='SWE')

        self._add_user_rows(chalmers_user, 'chalmers', time)

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
        t = self.max_time(True)
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
