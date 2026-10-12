"""Offline tests for strict shortname extraction and row rejection.

A name cell's link must be exactly the entity's page; anything else is
rejected (logged with a REJECT prefix that progolympstatus counts) instead of
being stored under a wrong shortname. No network; DB is in-memory.
Run standalone: `python3 tests/test_shortnames.py` (no pytest dependency).
"""
import contextlib
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import KattisDbConn
from db.kattis_db_conn import ScrapeRejected, shortname_from_href
from problem_scraper.scraper import ProblemScraper


def test_shortname_from_href():
    ok = [('user', 'users/joshua-andersson', 'joshua-andersson'),
          ('user', 'users/e0134149', 'e0134149'),
          ('affiliation', 'affiliations/chalmers.se', 'chalmers.se'),
          ('country', 'countries/SWE', 'SWE'),
          ('subdivision', 'countries/SWE/AB', 'SWE/AB')]
    for kind, href, sn in ok:
        assert shortname_from_href(kind, href) == sn, (kind, href)
    bad = [('user', None), ('user', ''), ('user', 'countries/SWE'), ('user', 'users/x/submissions'),
           ('user', 'users/x?tab=1'), ('user', 'https://open.kattis.com/users/x'),
           ('country', 'countries/SWE/AB'), ('subdivision', 'countries/SWE'),
           ('affiliation', 'users/chalmers.se')]
    for kind, href in bad:
        assert shortname_from_href(kind, href) is None, (kind, href)


def _user_row(rank, name, href, score):
    # (text, slug) cells as Scraper.parse_cell returns them.
    return [(str(rank), None), (name, href), ('Sweden', 'countries/SWE'),
            ('Chalmers', 'affiliations/chalmers.se'), (str(score), None)]


def _write(conn, rows):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        conn.add_user_rows(rows, 'chalmers', 1000)
    return out.getvalue()


def test_bad_row_rejected_rest_written():
    c = KattisDbConn(':memory:')
    log = _write(c, [_user_row(1, 'A', 'users/a', 10), _user_row(2, 'B', 'countries/SWE', 9)])
    assert c.conn.execute('SELECT shortname FROM user_obs').fetchall() == [('a',)]
    assert "REJECT user_obs ctx=chalmers href='countries/SWE'" in log, log
    assert 'WROTE user_obs ctx=chalmers rows=1 rejected=1' in log, log


def test_all_rejected_raises_and_writes_nothing():
    c = KattisDbConn(':memory:')
    try:
        _write(c, [_user_row(1, 'A', None, 10), _user_row(2, 'B', 'countries/SWE', 9)])
    except ScrapeRejected:
        pass
    else:
        raise AssertionError('expected ScrapeRejected')
    assert c.conn.execute('SELECT COUNT(*) FROM user_obs').fetchone()[0] == 0
    assert c.conn.execute('SELECT COUNT(*) FROM entities').fetchone()[0] == 0


def test_empty_table_is_not_a_rejection():
    c = KattisDbConn(':memory:')
    assert 'WROTE user_obs ctx=chalmers rows=0 rejected=0' in _write(c, [])


def _toplist_html(user_cells):
    rows = ''.join(f'<tr><td>{i + 1}</td><td>{cell}</td><td>0.01 s</td><td>C++</td><td>2024-01-01</td></tr>'
                   for i, cell in enumerate(user_cells))
    return (f'<div id="toplist_runtime_0"><table class="table2 "><tbody>{rows}</tbody></table></div>'
            '<div id="toplist_runtime_1"></div>')


def test_toplist_hidden_user_kept_bad_link_rejected():
    ps = ProblemScraper.__new__(ProblemScraper)
    rows, bad = ps._parse_toplist(_toplist_html([
        '<a href="/users/alice">Alice</a>',
        'Hidden user',
        '<a href="/countries/SWE">Bob</a>',
        'Carol',                                  # no link but not hidden
    ]), 'runtime')
    assert [(r[0], r[1], r[2]) for r in rows] == [(1, 'alice', 'Alice'), (2, None, 'Hidden user')], rows
    assert bad == ['countries/SWE', None], bad


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for t in tests:
        t()
        print(f'ok  {t.__name__}')
    print(f'\n{len(tests)} passed')


if __name__ == '__main__':
    _run_all()
