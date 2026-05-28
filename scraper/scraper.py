import re

from .http_client import EntityGone, KattisHttpClient


_USER_RANK_RE   = re.compile(r'<span class="info_label">Rank</span><span class="important_text">([^<]+)</span>')
_USER_SCORE_RE  = re.compile(r'<span class="info_label">Score</span><span class="important_text">([^<]+)</span>')
_USER_NAME_RE   = re.compile(r'<span class="image_info-text-main[^"]*"[^>]*>([^<]+)</span>')


class Scraper(KattisHttpClient):
    # ---- per-URL handlers ---------------------------------------------------

    def scrape_global_affiliations(self, time=None):
        rows = self.download_tables("https://open.kattis.com/ranklist/affiliations")[0]
        for r in rows:
            # Merge subdiv (r[3]) into country (r[2]) when present; drop subdiv cell.
            if r[3][0] != '':
                r[2] = (r[2][0] + " " + r[3][0], r[2][1])
            r.pop(3)
        self.kattis_conn.add_affiliation_rows(rows, 'global', self._ts(time))

    def scrape_global_users(self, time=None):
        rows = self.download_tables("https://open.kattis.com/ranklist")[0]
        self.kattis_conn.add_user_rows(rows, 'global', self._ts(time))

    def scrape_global_countries(self, time=None):
        rows = self.download_tables("https://open.kattis.com/ranklist/countries")[0]
        self.kattis_conn.add_country_rows(rows, 'global', self._ts(time))

    def scrape_global_languages(self, time=None):
        rows = self.download_tables("https://open.kattis.com/ranklist/languages")[0]
        self.kattis_conn.add_language_rows(rows, 'global', self._ts(time))

    def scrape_country(self, slug, context, time=None):
        """Discovery scrape of /countries/<slug>. Used for SWE today; any
        country with discover_users/discover_affiliations gets one of these."""
        tables = self.download_tables(f"https://open.kattis.com/countries/{slug}")
        ts = self._ts(time)
        self.kattis_conn.add_affiliation_rows( tables[0], context, ts, force_tracked=True)
        self.kattis_conn.add_user_rows(        tables[1], context, ts, force_tracked=True)
        self.kattis_conn.add_subdivision_rows( tables[2], context, ts, country=slug, force_tracked=True)

    def scrape_affiliation(self, slug, display_name, context, time=None):
        """Discovery scrape of /affiliations/<slug>. Used for chalmers today;
        any affiliation with discover_users gets one of these."""
        rows = self.download_tables(f"https://open.kattis.com/affiliations/{slug}")[0]
        for r in rows:
            r.insert(3, (display_name, f"affiliations/{slug}"))
        self.kattis_conn.add_user_rows(rows, context, self._ts(time), force_tracked=True)

    # Backwards-compatible aliases for tests / phase-2a callsites:
    def scrape_swe(self, time=None):
        self.scrape_country('SWE', 'swe', time)

    def scrape_chalmers(self, time=None):
        self.scrape_affiliation('chalmers.se', 'Chalmers University of Technology', 'chalmers', time)

    def scrape_user(self, shortname, time=None):
        """Per-user backstop: scrape /users/<slug> for current global rank+score."""
        html = self.download_html(f"https://open.kattis.com/users/{shortname}")
        rank_m  = _USER_RANK_RE.search(html)
        score_m = _USER_SCORE_RE.search(html)
        name_m  = _USER_NAME_RE.search(html)
        if not (rank_m and score_m and name_m):
            raise RuntimeError(
                f"could not parse user page for {shortname}: "
                f"rank={bool(rank_m)} score={bool(score_m)} name={bool(name_m)}"
            )
        rank = int(rank_m.group(1).replace(',', ''))
        score = float(score_m.group(1).replace(',', ''))
        display_name = name_m.group(1).strip()
        self.kattis_conn.add_user_backstop(shortname, display_name, rank, score, self._ts(time))

    def scrape(self, time=None):
        """Run all per-URL handlers in sequence. Used by tests and one-shot runs."""
        self.scrape_global_affiliations(time)
        self.scrape_global_users(time)
        self.scrape_global_countries(time)
        self.scrape_global_languages(time)
        self.scrape_swe(time)
        self.scrape_chalmers(time)
