import sys
import time
import traceback
from collections import namedtuple

from .scraper import Scraper

SCRAPE_INTERVAL_SECONDS = 600
JOB_DUE_AFTER_SECONDS = 24 * 3600
MAX_CONSECUTIVE_FAILS = 10


Job = namedtuple('Job', 'name handler due_table due_context')


def build_jobs(scraper):
    return [
        Job('global_affiliations', scraper.scrape_global_affiliations, 'affiliation_obs', 'global'),
        Job('global_users',        scraper.scrape_global_users,        'user_obs',        'global'),
        Job('global_countries',    scraper.scrape_global_countries,    'country_obs',     'global'),
        Job('global_languages',    scraper.scrape_global_languages,    'language_obs',    'global'),
        Job('swe',                 scraper.scrape_swe,                 'affiliation_obs', 'swe'),
        Job('chalmers',            scraper.scrape_chalmers,            'user_obs',        'chalmers'),
    ]


def job_due(conn, job, now_ts):
    row = conn.execute(
        f'SELECT MAX(timestamp) FROM {job.due_table} WHERE context=?',
        (job.due_context,)
    ).fetchone()
    last = row[0]
    return last is None or last < now_ts - JOB_DUE_AFTER_SECONDS


def pick_due_job(conn, jobs, now_ts):
    for j in jobs:
        if job_due(conn, j, now_ts):
            return j
    return None


def main():
    scraper = Scraper()
    jobs = build_jobs(scraper)
    fails = {j.name: 0 for j in jobs}

    while True:
        now_ts = int(time.time())
        job = pick_due_job(scraper.kattis_conn.conn, jobs, now_ts)
        if job is None:
            time.sleep(SCRAPE_INTERVAL_SECONDS)
            continue

        print(f'scraping {job.name}...', flush=True)
        try:
            job.handler()
            print(f'ok ({job.name})', flush=True)
            if fails[job.name]:
                print(f'{job.name}: recovered after {fails[job.name]} failure(s)', flush=True)
                fails[job.name] = 0
        except Exception as e:
            print(f'{job.name} failed!\n\n{e}\n', flush=True)
            traceback.print_exc()
            fails[job.name] += 1
            if fails[job.name] >= MAX_CONSECUTIVE_FAILS:
                print(f'{job.name}: {fails[job.name]} consecutive fails; exiting for systemd to restart.', flush=True)
                sys.exit(1)

        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
