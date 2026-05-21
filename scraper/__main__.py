import sys
import time
import traceback
from datetime import datetime

from .scraper import Scraper

SCRAPE_INTERVAL_SECONDS = 600
MAX_CONSECUTIVE_FAILS = 10


def main():
    scraper = Scraper()
    fails = 0
    while True:
        prev = scraper.kattis_conn.max_time()
        today = datetime.now().date()
        if prev is None or prev.date() != today:
            try:
                print("scraping...", flush=True)
                scraper.scrape()
                print("ok!", flush=True)
                if fails:
                    print("scraped ok after %d failure(s)" % fails, flush=True)
                    fails = 0
            except Exception as e:
                print("Scraping failed!\n\n", e, "\n", flush=True)
                traceback.print_exc()
                fails += 1
                if fails >= MAX_CONSECUTIVE_FAILS:
                    print("%d consecutive fails; exiting for systemd to restart." % fails, flush=True)
                    sys.exit(1)
        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
