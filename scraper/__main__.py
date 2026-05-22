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

    def due(table, context):
        def check(now_ts):
            row = conn.execute(
                f'SELECT MAX(timestamp) FROM {table} WHERE context=?', (context,)
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

    # Per-user backstop: tracked alive users.
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


def pick_due_job(jobs, now_ts):
    for j in jobs:
        if j.is_due(now_ts):
            return j
    return None


def count_due(jobs, now_ts):
    return sum(1 for j in jobs if j.is_due(now_ts))


def compute_interval(n_due):
    if n_due <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return max(MIN_INTERVAL_SECONDS, min(DEFAULT_INTERVAL_SECONDS, 86400 // n_due))


def main():
    scraper = Scraper()
    fixed_fails = {}

    while True:
        now_ts = int(time.time())
        jobs = build_jobs(scraper, now_ts)
        job = pick_due_job(jobs, now_ts)

        if job is None:
            time.sleep(DEFAULT_INTERVAL_SECONDS)
            continue

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

        n_due = count_due(jobs, int(time.time()))
        time.sleep(compute_interval(n_due))


if __name__ == '__main__':
    main()
