import sys
import time
import traceback

from .scraper import ProblemScraper
from scraper.http_client import EntityGone

POLITE_INTERVAL_SECONDS = 60            # between detail-page fetches
ALIVE_WINDOW_SECONDS = 10 * 86400       # decay after 10 days off the listing
MAX_LIST_FAILS = 10                     # consecutive list failures -> exit


def discover(scraper):
    """Scrape the full /problems listing, registering every problem entity.
    Returns the number of problems seen."""
    page = 1
    total = 0
    while True:
        rows = scraper.scrape_problem_list(page)
        if not rows:
            break
        scraper.kattis_conn.register_problems(rows, int(time.time()))
        total += len(rows)
        page += 1
    return total


def main():
    scraper = ProblemScraper()
    list_fails = 0

    while True:
        # --- discovery: refresh the full problem list once per rotation ------
        try:
            n = discover(scraper)
            print(f'discovered {n} problems', flush=True)
            list_fails = 0
        except Exception as e:
            list_fails += 1
            print(f'problem list scrape failed ({list_fails}/{MAX_LIST_FAILS}): {e}', flush=True)
            traceback.print_exc()
            if list_fails >= MAX_LIST_FAILS:
                print('too many consecutive list failures; exiting for systemd restart.', flush=True)
                sys.exit(1)
            time.sleep(POLITE_INTERVAL_SECONDS)
            continue

        # --- observation: walk problems stalest-first, one detail page each --
        alive_since = int(time.time()) - ALIVE_WINDOW_SECONDS
        problems = scraper.kattis_conn.problems_to_scrape(alive_since)
        print(f'rotation: {len(problems)} problems to scrape', flush=True)

        for slug, _display_name in problems:
            try:
                scraper.scrape_problem(slug)
                print(f'ok ({slug})', flush=True)
            except EntityGone as e:
                # Retired problem: don't bump last_seen_alive; it decays out
                # if it also stops appearing in the listing.
                print(f'{slug}: gone ({e})', flush=True)
            except Exception as e:
                print(f'{slug} failed!\n  {e}', flush=True)
                traceback.print_exc()
            time.sleep(POLITE_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
