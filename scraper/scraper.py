import re
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import db


_HREF_RE        = re.compile(r'<a [^>]*href="([^"]+)"')
_USER_RANK_RE   = re.compile(r'<span class="info_label">Rank</span><span class="important_text">([^<]+)</span>')
_USER_SCORE_RE  = re.compile(r'<span class="info_label">Score</span><span class="important_text">([^<]+)</span>')
_USER_NAME_RE   = re.compile(r'<span class="image_info-text-main[^"]*"[^>]*>([^<]+)</span>')


class EntityGone(Exception):
    """Raised when a per-entity scrape hits HTTP 404 — the entity no longer
    exists on Kattis and should not have its last_seen_alive bumped."""


class Scraper:
    def __init__(self):
        self.kattis_conn = db.KattisDbConn("db/kattis.db")

    def download_html(self, url):
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0 (Contact: Joshua Andersson)'})
        try:
            return urlopen(req).read().decode('utf-8')
        except HTTPError as e:
            if e.code == 404:
                raise EntityGone(url) from e
            raise

    def parse_cell(self, cell):
        # Capture the slug from the first <a href="..."> in the cell, if any.
        # Returns the path without the leading slash, e.g. "users/joshua-andersson".
        m = _HREF_RE.search(cell)
        slug = m.group(1).lstrip('/') if m else None

        # Strip tags, normalize whitespace.
        r = []
        cnt = 0
        for c in cell:
            if c == '<': cnt += 1
            if cnt == 0: r.append(c)
            if c == '>': cnt -= 1
        text = "".join(r).strip().replace('\n', '')
        while True:
            text2 = text.replace('  ', ' ')
            if text2 == text: break
            text = text2
        return (text, slug)

    def get_tables(self, webpage):
        tables = []
        i = 0
        while i < len(webpage):
            if webpage.startswith(' class="table2 "', i):
                while not webpage.startswith('<tbody>', i): i += 1
                table = []
                while True:
                    while not webpage.startswith('<tr', i) and not webpage.startswith('</tbody>', i): i += 1
                    if webpage.startswith('</tbody>', i): break
                    row = []
                    while True:
                        while not webpage.startswith('<td', i) and not webpage.startswith('</tr>', i): i += 1
                        if webpage.startswith('</tr>', i): break
                        while webpage[i] != '>': i += 1
                        i += 1
                        cell = []
                        while not webpage.startswith('</td>', i):
                            cell.append(webpage[i])
                            i += 1
                        row.append(self.parse_cell("".join(cell)))
                    table.append(row)
                tables.append(table)
            i += 1
        return tables

    def download_tables(self, url):
        return self.get_tables(self.download_html(url))

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

    @staticmethod
    def _ts(time):
        if time is None: time = datetime.now()
        return int(time.timestamp())
