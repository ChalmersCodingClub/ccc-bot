"""Offline tests for entity bookkeeping (`entities.first_seen/last_seen_alive`).

Regression for a NULL bug: `_touch_entity` updated `last_seen_alive` with
SQLite's multi-arg `max(old, new)`, which returns NULL if ANY argument is NULL.
An entity created by `set_flags` (manual tracking, `/track-user`) starts with
NULL `first_seen`/`last_seen_alive`, so they stayed NULL forever no matter how
often it was observed — and since the scheduler treats NULL as alive, such an
entity could never decay (found in prod 2026-11-08: 13 users, 10 affiliations).
Run standalone: `python3 tests/test_entities.py` (no pytest dependency).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import KattisDbConn


def _seen(c, kind, sn):
    return c.conn.execute(
        'SELECT tracked, first_seen, last_seen_alive FROM entities WHERE kind=? AND shortname=?',
        (kind, sn)).fetchone()


def test_set_flags_entity_gets_dates_on_observation():
    c = KattisDbConn(':memory:')
    c.set_flags('user', 'alice', tracked=1)
    assert _seen(c, 'user', 'alice') == (1, None, None)
    c.add_user_backstop('alice', 'Alice', 500, 1000.0, 1000)
    assert _seen(c, 'user', 'alice') == (1, 1000, 1000)
    c.add_user_backstop('alice', 'Alice', 499, 1001.0, 2000)
    assert _seen(c, 'user', 'alice') == (1, 1000, 2000), 'first_seen must not move'


def test_last_seen_alive_never_goes_backwards():
    c = KattisDbConn(':memory:')
    c.add_user_backstop('bob', 'Bob', 1, 1.0, 2000)
    c.add_user_backstop('bob', 'Bob', 1, 1.0, 1500)   # older timestamp
    assert _seen(c, 'user', 'bob') == (1, 2000, 2000)


def test_tracked_is_sticky():
    # An untracked observation (e.g. a country-observe scrape) must not clear
    # tracked; a tracked one sets it.
    c = KattisDbConn(':memory:')
    c.set_flags('user', 'carol', tracked=1)
    c._touch_entity('user', 'carol', 'Carol', 1000, qualifies_tracked=False)
    assert _seen(c, 'user', 'carol')[0] == 1
    c._touch_entity('user', 'dave', 'Dave', 1000, qualifies_tracked=False)
    assert _seen(c, 'user', 'dave')[0] == 0
    c._touch_entity('user', 'dave', 'Dave', 2000, qualifies_tracked=True)
    assert _seen(c, 'user', 'dave') == (1, 1000, 2000)


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for t in tests:
        t()
        print(f'ok  {t.__name__}')
    print(f'\n{len(tests)} passed')


if __name__ == '__main__':
    _run_all()
