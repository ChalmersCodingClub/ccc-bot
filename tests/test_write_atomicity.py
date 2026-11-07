"""Offline tests: DB writes are all-or-nothing, and HTTP requests time out.

A scrape that fails midway must leave nothing behind — in particular a
problem_obs row committed without its toplists reads as "toplist empty = tied"
(see CLAUDE.md). No network; DBs are in-memory or in a temp dir.
Run standalone: `python3 tests/test_write_atomicity.py` (no pytest dependency).
"""
import contextlib
import io
import os
import sqlite3
import sys
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scraper.http_client as http_client
from db import KattisDbConn
from problem_scraper.scraper import BlankStatsPage, ProblemScraper


def _user_row(rank, name, href, score):
    # (text, slug) cells as Scraper.parse_cell returns them.
    return [(str(rank), None), (name, href), ('Sweden', 'countries/SWE'),
            ('Chalmers', 'affiliations/chalmers.se'), (str(score), None)]


def test_helper_failure_leaves_no_pending_writes():
    # Row 1 touches its entity, then row 2 fails to parse. The touch must be
    # rolled back, not left pending for the next commit to persist (that would
    # bump last_seen_alive with no observation behind it).
    c = KattisDbConn(':memory:')
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            c.add_user_rows([_user_row(1, 'A', 'users/a', 10), _user_row('x', 'B', 'users/b', 9)],
                            'chalmers', 1000)
    except ValueError:
        pass
    else:
        raise AssertionError('expected ValueError')
    assert c.conn.execute('SELECT COUNT(*) FROM entities').fetchone()[0] == 0
    assert c.conn.execute('SELECT COUNT(*) FROM user_obs').fetchone()[0] == 0


def test_nested_transaction_commits_only_at_outermost():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, 'k.db')
        c = KattisDbConn(path)
        other = sqlite3.connect(path)
        count = lambda: other.execute('SELECT COUNT(*) FROM problem_verdict').fetchone()[0]
        with c.transaction():
            c.add_problem_verdicts('p', 1000, [('Accepted', 1)])
            assert count() == 0, 'inner helper committed early'
        assert count() == 1


def _stats_html():
    return ('<h1>Foo — Problem Statistics</h1>'
            '<script id="status-donut-data" type="application/json">'
            '{"labels": ["Accepted", "Wrong Answer"], "data": [5, 3]}</script>'
            '<div id="toplist_runtime_0"><table class="table2 "><tbody>'
            '<tr><td>1</td><td><a href="/users/alice">Alice</a></td><td>0.01 s</td>'
            '<td>C++</td><td>2024-01-01</td></tr>'
            '</tbody></table></div><div id="toplist_runtime_1"></div>')


def test_problem_scrape_is_all_or_nothing():
    ps = ProblemScraper.__new__(ProblemScraper)
    ps.kattis_conn = KattisDbConn(':memory:')
    ps.download_html = lambda url: _stats_html()

    def boom(*args, **kwargs):
        raise RuntimeError('toplist write failed')
    ps.kattis_conn.add_problem_toplist = boom
    try:
        ps.scrape_problem('foo', time=None)
    except RuntimeError:
        pass
    else:
        raise AssertionError('expected RuntimeError')
    for t in ('problem_obs', 'problem_verdict', 'entities'):
        n = ps.kattis_conn.conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        assert n == 0, f'{t} kept {n} rows from a failed scrape'

    # Sanity: the same page writes everything when nothing fails.
    del ps.kattis_conn.add_problem_toplist
    ps.scrape_problem('foo')
    q = lambda sql: ps.kattis_conn.conn.execute(sql).fetchone()[0]
    assert q('SELECT COUNT(*) FROM problem_obs') == 1
    assert q('SELECT COUNT(*) FROM problem_verdict') == 2
    assert q("SELECT user_shortname FROM problem_toplist WHERE kind='fastest'") == 'alice'


def _t(ts):
    return datetime.fromtimestamp(ts)


def _summary_html(submissions):
    # Trimmed live markup of the condensed summary table.
    return ('<table class="table2 condensed mt-5">'
            f'<tr><td style="min-width:230px">Submissions</td><td><span>{submissions}</span></td></tr>'
            f'<tr><td>Accepted submissions</td><td><span>{submissions // 2}</span></td></tr>'
            f'<tr><td>Authors</td><td><span>{submissions // 4}</span></td></tr>'
            '</table>')


def test_blank_stats_page_is_not_stored():
    # Kattis glitch (seen in prod, e.g. 2026-07-27, 2026-11-05 01:00 UTC): the
    # page comes back with zero counts, no donut and no toplists. Stored, its
    # empty toplists would read as "tied / no holder", so it must be rejected
    # once the problem has real stats on record.
    ps = ProblemScraper.__new__(ProblemScraper)
    ps.kattis_conn = KattisDbConn(':memory:')
    ps.download_html = lambda url: _summary_html(172) + _stats_html()
    ps.scrape_problem('foo', time=_t(1000))

    ps.download_html = lambda url: '<h1>Foo — Problem Statistics</h1>' + _summary_html(0)
    try:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            ps.scrape_problem('foo', time=_t(2000))
    except BlankStatsPage:
        pass
    else:
        raise AssertionError('expected BlankStatsPage')
    assert 'BLANK-PAGE problem_obs ctx=foo' in out.getvalue()
    q = lambda sql: ps.kattis_conn.conn.execute(sql).fetchone()[0]
    assert q('SELECT COUNT(*) FROM problem_obs') == 1, 'blank page was stored'
    assert q('SELECT MAX(timestamp) FROM problem_obs') == 1000
    assert q('SELECT COUNT(*) FROM problem_toplist') == 1

    # A real page afterwards is stored as usual.
    ps.download_html = lambda url: _summary_html(173) + _stats_html()
    ps.scrape_problem('foo', time=_t(3000))
    assert q('SELECT COUNT(*) FROM problem_obs') == 2

    # Still rejected when the latest stored row is itself an old glitch (0
    # submissions, stored before this check existed): compare to the max.
    ps.kattis_conn.add_problem_obs('foo', 'Foo', 4000, None, None, 0, 0, 0, 0)
    ps.download_html = lambda url: '<h1>Foo — Problem Statistics</h1>' + _summary_html(0)
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            ps.scrape_problem('foo', time=_t(5000))
    except BlankStatsPage:
        pass
    else:
        raise AssertionError('expected BlankStatsPage after a stored glitch row')


def test_never_submitted_problem_is_stored():
    # Zero submissions is legitimate for a problem with no prior stats (new /
    # unsolved): no previous count to contradict it, so it's written.
    ps = ProblemScraper.__new__(ProblemScraper)
    ps.kattis_conn = KattisDbConn(':memory:')
    ps.download_html = lambda url: '<h1>New — Problem Statistics</h1>' + _summary_html(0)
    ps.scrape_problem('new', time=_t(1000))
    ps.scrape_problem('new', time=_t(2000))
    n = ps.kattis_conn.conn.execute('SELECT COUNT(*) FROM problem_obs').fetchone()[0]
    assert n == 2


def test_download_html_sets_timeout():
    seen = {}

    class _Resp:
        def read(self):
            return b'ok'

    def fake_urlopen(req, timeout=None):
        seen['timeout'] = timeout
        return _Resp()

    real = http_client.urlopen
    http_client.urlopen = fake_urlopen
    try:
        client = http_client.KattisHttpClient.__new__(http_client.KattisHttpClient)
        assert client.download_html('https://example.invalid/') == 'ok'
    finally:
        http_client.urlopen = real
    assert seen['timeout'] == http_client.HTTP_TIMEOUT_SECONDS


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for t in tests:
        t()
        print(f'ok  {t.__name__}')
    print(f'\n{len(tests)} passed')


if __name__ == '__main__':
    _run_all()
