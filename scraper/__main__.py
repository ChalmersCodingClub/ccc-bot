import sys
import time
import traceback
from collections import namedtuple

from .scraper import Scraper, EntityGone

DEFAULT_INTERVAL_SECONDS = 600          # 10 min: ceiling when little is due
MIN_INTERVAL_SECONDS = 30               # polite floor
JOB_DUE_AFTER_SECONDS = 24 * 3600
ALIVE_WINDOW_SECONDS = 10 * 86400       # decay after 10 days of silence
MAX_FIXED_JOB_FAILS = 10
# A full ranklist scrape writes many rows under one timestamp; the per-user
# backstop writes *single* context='global' rows into user_obs. The fixed-job
# due-check must ignore those single-row writes, else backstop activity keeps
# the global_users due-check perpetually "fresh" and the top-100 ranklist job
# never runs. (This deadlock silently stalled global_users 2026-05-23..07-11.)
FIXED_SCRAPE_MIN_ROWS = 10

# Context name used for back-compat discovery scrapes. New discovery targets
# (e.g., country=USA, affiliation=kth.se) use their shortname as the context.
_LEGACY_CONTEXT = {
    ('country', 'SWE'): 'swe',
    ('affiliation', 'chalmers.se'): 'chalmers',
}


Job = namedtuple('Job', 'name handler is_due is_fixed')


def _context_for(kind, shortname):
    return _LEGACY_CONTEXT.get((kind, shortname), shortname)


def _build_fixed_jobs(scraper):
    conn = scraper.kattis_conn.conn

    def due(table, context, min_rows=FIXED_SCRAPE_MIN_ROWS):
        # Only count *batch* scrapes (a full ranklist shares one timestamp
        # across many rows); single-row backstop writes are excluded by the
        # HAVING floor so they can't mask this job's staleness.
        def check(now_ts):
            row = conn.execute(
                f'SELECT MAX(timestamp) FROM '
                f'(SELECT timestamp FROM {table} WHERE context=? '
                f' GROUP BY timestamp HAVING COUNT(*) >= ?)',
                (context, min_rows)
            ).fetchone()
            t = row[0]
            return t is None or t < now_ts - JOB_DUE_AFTER_SECONDS
        return check

    return [
        Job('global_affiliations', scraper.scrape_global_affiliations, due('affiliation_obs', 'global'), True),
        Job('global_users',        scraper.scrape_global_users,        due('user_obs',        'global'), True),
        Job('global_countries',    scraper.scrape_global_countries,    due('country_obs',     'global'), True),
        Job('global_languages',    scraper.scrape_global_languages,    due('language_obs',    'global'), True),
    ]


def _build_dynamic_jobs(scraper, now_ts):
    """Discovery + backstop jobs derived from the entities table.
    Excludes silent entities (last_seen_alive older than ALIVE_WINDOW_SECONDS)."""
    conn = scraper.kattis_conn.conn
    alive_since = now_ts - ALIVE_WINDOW_SECONDS
    jobs = []

    # Discovery: tracked countries/affiliations with discover_* flags.
    discovery_rows = conn.execute('''
        SELECT kind, shortname, display_name, discover_users, discover_affiliations
        FROM entities
        WHERE tracked=1
          AND (discover_users=1 OR discover_affiliations=1)
          AND (last_seen_alive IS NULL OR last_seen_alive > ?)
    ''', (alive_since,)).fetchall()

    def country_due(ctx):
        def check(now_ts):
            t = conn.execute(
                'SELECT MAX(timestamp) FROM affiliation_obs WHERE context=?', (ctx,)
            ).fetchone()[0]
            return t is None or t < now_ts - JOB_DUE_AFTER_SECONDS
        return check

    def affiliation_due(ctx):
        def check(now_ts):
            t = conn.execute(
                'SELECT MAX(timestamp) FROM user_obs WHERE context=?', (ctx,)
            ).fetchone()[0]
            return t is None or t < now_ts - JOB_DUE_AFTER_SECONDS
        return check

    for kind, sn, dn, du, da in discovery_rows:
        ctx = _context_for(kind, sn)
        if kind == 'country':
            handler = (lambda s=sn, c=ctx: scraper.scrape_country(s, c))
            jobs.append(Job(f'country/{sn}', handler, country_due(ctx), False))
        elif kind == 'affiliation':
            handler = (lambda s=sn, n=dn, c=ctx: scraper.scrape_affiliation(s, n, c))
            jobs.append(Job(f'affiliation/{sn}', handler, affiliation_due(ctx), False))
        # subdivision / language not yet supported for discovery in 2b

    # Observe-only: record an entity's user toplist (and, for countries, its
    # affiliation aggregate list) into *_obs without tracking those sub-entities
    # (no per-user backstop fan-out) and without discovering them. Alternative
    # to discover_*; gated on observe_* alone (independent of tracked).
    observe_rows = conn.execute('''
        SELECT kind, shortname, display_name, observe_users, observe_affiliations
        FROM entities
        WHERE (observe_users=1 OR observe_affiliations=1)
          AND (last_seen_alive IS NULL OR last_seen_alive > ?)
    ''', (alive_since,)).fetchall()

    for kind, sn, dn, ou, oa in observe_rows:
        ctx = _context_for(kind, sn)
        if kind == 'country':
            handler = (lambda s=sn, c=ctx, u=bool(ou), a=bool(oa):
                       scraper.scrape_country(s, c, track=False, observe_users=u, observe_affiliations=a))
            # Both tables share a timestamp; due-check whichever we write.
            due = affiliation_due(ctx) if ou else country_due(ctx)
            jobs.append(Job(f'country-observe/{sn}', handler, due, False))
        elif kind == 'affiliation':
            handler = (lambda s=sn, n=dn, c=ctx: scraper.scrape_affiliation(s, n, c, track=False))
            jobs.append(Job(f'affiliation-users/{sn}', handler, affiliation_due(ctx), False))

    # Per-user backstop: tracked alive users. Order is irrelevant — main()
    # round-robins the dynamic jobs by name, so fairness doesn't depend on the
    # list order here (and a stalest-first order would sort a permanently-due
    # 404 user to the front, which is exactly what we don't want).
    user_rows = conn.execute('''
        SELECT shortname, display_name FROM entities
        WHERE kind='user' AND tracked=1
          AND (last_seen_alive IS NULL OR last_seen_alive > ?)
    ''', (alive_since,)).fetchall()

    def user_backstop_due(sn):
        def check(now_ts):
            t = conn.execute(
                "SELECT MAX(timestamp) FROM user_obs WHERE shortname=? AND context='global'",
                (sn,)
            ).fetchone()[0]
            return t is None or t < now_ts - JOB_DUE_AFTER_SECONDS
        return check

    for sn, dn in user_rows:
        handler = (lambda s=sn: scraper.scrape_user(s))
        jobs.append(Job(f'user/{sn}', handler, user_backstop_due(sn), False))

    return jobs


def build_jobs(scraper, now_ts):
    return _build_fixed_jobs(scraper) + _build_dynamic_jobs(scraper, now_ts)


def pick_job(jobs, is_due, last_dyn_name):
    """Choose the next job to run.

    Fixed ranklist jobs keep priority (they are due only ~once/24h and the
    fixed-fail counter in main() bounces the process if one wedges). Dynamic
    jobs (backstop/discovery/observe) are scheduled round-robin by their stable,
    unique name so that a single job whose due-check never clears — e.g. a 404
    user (EntityGone) that never records an observation — cannot monopolize the
    'one job per tick' loop and starve everything after it.

    `is_due` is a {name: bool} map evaluated once per tick. `last_dyn_name` is
    the name of the dynamic job run last tick (the round-robin cursor).
    """
    for j in jobs:
        if j.is_fixed and is_due[j.name]:
            return j
    dyn = sorted((j for j in jobs if not j.is_fixed and is_due[j.name]),
                 key=lambda j: j.name)
    if not dyn:
        return None
    if last_dyn_name is not None:
        for j in dyn:
            if j.name > last_dyn_name:
                return j
    return dyn[0]  # first run, or wrap around past the last name


def compute_interval(n_due):
    if n_due <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return max(MIN_INTERVAL_SECONDS, min(DEFAULT_INTERVAL_SECONDS, 86400 // n_due))


def main():
    scraper = Scraper()
    fixed_fails = {}
    last_dyn_name = None  # round-robin cursor over dynamic jobs

    while True:
        now_ts = int(time.time())
        jobs = build_jobs(scraper, now_ts)
        is_due = {j.name: j.is_due(now_ts) for j in jobs}
        job = pick_job(jobs, is_due, last_dyn_name)

        if job is None:
            time.sleep(DEFAULT_INTERVAL_SECONDS)
            continue

        # Advance the cursor before running, so a job that fails is still
        # stepped past next tick — this is what stops an unsatisfiable job
        # (e.g. a 404 user) from monopolizing the loop.
        if not job.is_fixed:
            last_dyn_name = job.name

        print(f'scraping {job.name}...', flush=True)
        try:
            job.handler()
            print(f'ok ({job.name})', flush=True)
            if job.is_fixed and fixed_fails.get(job.name):
                print(f'{job.name}: recovered after {fixed_fails[job.name]} failure(s)', flush=True)
                fixed_fails[job.name] = 0
        except EntityGone as e:
            # Entity is gone from Kattis. Don't update last_seen_alive; decay
            # will eventually drop this job from the dynamic set.
            print(f'{job.name}: gone ({e})', flush=True)
        except Exception as e:
            print(f'{job.name} failed!\n  {e}', flush=True)
            traceback.print_exc()
            if job.is_fixed:
                fixed_fails[job.name] = fixed_fails.get(job.name, 0) + 1
                if fixed_fails[job.name] >= MAX_FIXED_JOB_FAILS:
                    print(f'{job.name}: {fixed_fails[job.name]} consecutive fails; exiting for systemd restart.', flush=True)
                    sys.exit(1)
            # else: dynamic job, transient; just continue. Next tick may retry.

        n_due = sum(is_due.values())
        time.sleep(compute_interval(n_due))


if __name__ == '__main__':
    main()
