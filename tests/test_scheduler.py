"""Offline tests for the scraper's round-robin job scheduler.

Pure-function tests for `scraper.__main__.pick_job` — no DB, no network.
Run standalone: `python3 tests/test_scheduler.py` (no pytest dependency).

These lock in the fix for the bug where one unsatisfiable dynamic job (a 404
user whose due-check never clears) monopolized the "one job per tick" loop and
starved every job after it.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.__main__ import Job, pick_job


def _job(name, is_fixed=False):
    # handler/is_due are unused by pick_job (it reads the is_due map), so stub.
    return Job(name, handler=None, is_due=None, is_fixed=is_fixed)


def test_fixed_priority():
    """A due fixed job is returned before any due dynamic job."""
    jobs = [_job('global_users', is_fixed=True), _job('user/a'), _job('user/b')]
    is_due = {j.name: True for j in jobs}
    assert pick_job(jobs, is_due, last_dyn_name=None).name == 'global_users'

    # ...but not when the fixed job isn't due.
    is_due['global_users'] = False
    assert pick_job(jobs, is_due, last_dyn_name=None).name == 'user/a'


def test_none_when_nothing_due():
    jobs = [_job('global_users', is_fixed=True), _job('user/a')]
    is_due = {j.name: False for j in jobs}
    assert pick_job(jobs, is_due, last_dyn_name=None) is None


def test_wrap_around():
    """After the lexicographically-last dynamic job, the next pick is the first."""
    jobs = [_job('user/a'), _job('user/b'), _job('user/c')]
    is_due = {j.name: True for j in jobs}
    assert pick_job(jobs, is_due, last_dyn_name='user/c').name == 'user/a'
    assert pick_job(jobs, is_due, last_dyn_name='user/a').name == 'user/b'


def test_full_rotation_no_monopoly():
    """Round-robin invariant: with everything due, one job cannot be picked
    twice before every other due job is picked once.

    This is the direct regression for the old `pick_due_job`, which returned the
    same first-due job every tick. Over `len(jobs)` ticks (feeding the cursor
    back, nothing clearing), round-robin must yield one full rotation — every
    job exactly once.
    """
    all_names = sorted([f'user/{c}' for c in 'abcdefgh'] + ['user/oswa'])
    jobs = [_job(n) for n in all_names]
    is_due = {n: True for n in all_names}

    picks, last_dyn_name = [], None
    for _ in range(len(all_names)):
        job = pick_job(jobs, is_due, last_dyn_name)
        last_dyn_name = job.name
        picks.append(job.name)

    assert len(set(picks)) == len(all_names), f'not a full rotation: {picks}'
    assert set(picks) == set(all_names)


def test_no_starvation_with_permanent_wedge():
    """A permanently-due job (a 404 user that never records an observation) must
    not starve the others: every real job still gets served.

    Simulate main(): each served real job clears for 24h; the wedged job stays
    due forever. The old scheduler would have returned only the wedged job once
    it became first-due, starving everyone after it.

    The wedge MUST sort first: that is the position the old stalest-first
    ORDER BY put a never-observed user in, and it is the only position where
    first-due-wins actually starves. With the wedge anywhere else this test
    passes under the buggy scheduler too and proves nothing.
    """
    real = [f'user/{c}' for c in 'abcdefgh']
    wedged = 'user/-gone'  # permanently due, and sorts ahead of every real job
    all_names = sorted(real + [wedged])
    assert all_names[0] == wedged, 'wedge must sort first or this test is vacuous'
    jobs = [_job(n) for n in all_names]

    is_due = {n: True for n in all_names}
    last_dyn_name, served = None, set()
    for _ in range(len(all_names) + 2):  # one cycle is enough; slack for safety
        job = pick_job(jobs, is_due, last_dyn_name)
        last_dyn_name = job.name
        if job.name != wedged:
            served.add(job.name)
            is_due[job.name] = False  # cleared for 24h

    assert served == set(real), f'starved: {set(real) - served}'


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for t in tests:
        t()
        print(f'ok  {t.__name__}')
    print(f'\n{len(tests)} passed')


if __name__ == '__main__':
    _run_all()
