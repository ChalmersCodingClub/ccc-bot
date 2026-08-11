import re

from .http_client import EntityGone, KattisHttpClient


# `\s*` between the label and value spans is load-bearing: Kattis started
# emitting whitespace/newlines between them on 2026-07-30, which silently broke
# every per-user backstop scrape for 12 days (rank/score unmatched, name fine).
# Don't re-anchor these as adjacent.
_USER_RANK_RE   = re.compile(r'<span class="info_label">Rank</span>\s*<span class="important_text">([^<]+)</span>')
_USER_SCORE_RE  = re.compile(r'<span class="info_label">Score</span>\s*<span class="important_text">([^<]+)</span>')
_USER_NAME_RE   = re.compile(r'<span class="image_info-text-main[^"]*"[^>]*>([^<]+)</span>')


def parse_user_page(html):
    """Parse a /users/<slug> profile page -> (display_name, rank, score).

    Pure function (offline-testable). Raises ValueError naming which fields
    failed to match; the caller adds the slug for the log line.

    A user with no ranked submissions shows "-" for rank/score -> None, so the
    observation still gets written. Otherwise the write never happens, the user
    stays perpetually backstop-"due", and the scheduler wastes a slot on them
    every rotation.
    """
    rank_m  = _USER_RANK_RE.search(html)
    score_m = _USER_SCORE_RE.search(html)
    name_m  = _USER_NAME_RE.search(html)
    if not (rank_m and score_m and name_m):
        raise ValueError(
            f"rank={bool(rank_m)} score={bool(score_m)} name={bool(name_m)}"
        )

    def _num(txt, cast):
        txt = txt.strip()
        return None if txt in ('-', '', 'N/A') else cast(txt.replace(',', ''))

    return name_m.group(1).strip(), _num(rank_m.group(1), int), _num(score_m.group(1), float)


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

    def scrape_country(self, slug, context, time=None, track=True,
                       observe_users=False, observe_affiliations=False):
        """Scrape /countries/<slug>. With track=True (discovery): pull the
        affiliations, users and subdivisions tables and force_track them all —
        used for SWE and any country with discover_users/discover_affiliations.
        With track=False (observe-only): write the requested tables
        (`observe_users` → users toplist, `observe_affiliations` → affiliation
        aggregate list) with force_tracked=False (no backstop fan-out), and
        never touch the subdivisions table (no discovery, and sidesteps the
        missing-tables[2] crash on subdivision-less countries)."""
        tables = self.download_tables(f"https://open.kattis.com/countries/{slug}")
        ts = self._ts(time)
        if track:
            self.kattis_conn.add_affiliation_rows( tables[0], context, ts, force_tracked=True)
            self.kattis_conn.add_user_rows(        tables[1], context, ts, force_tracked=True)
            self.kattis_conn.add_subdivision_rows( tables[2], context, ts, country=slug, force_tracked=True)
            return
        if observe_affiliations:
            self.kattis_conn.add_affiliation_rows( tables[0], context, ts, force_tracked=False)
        if observe_users:
            self.kattis_conn.add_user_rows(        tables[1], context, ts, force_tracked=False)

    def scrape_affiliation(self, slug, display_name, context, time=None, track=True):
        """Scrape /affiliations/<slug>. track=True (discovery): force_track the
        users — used for chalmers and any affiliation with discover_users.
        track=False (observe-only): record the users toplist without tracking
        them (no backstop fan-out)."""
        rows = self.download_tables(f"https://open.kattis.com/affiliations/{slug}")[0]
        for r in rows:
            r.insert(3, (display_name, f"affiliations/{slug}"))
        self.kattis_conn.add_user_rows(rows, context, self._ts(time), force_tracked=track)

    # Backwards-compatible aliases for tests / phase-2a callsites:
    def scrape_swe(self, time=None):
        self.scrape_country('SWE', 'swe', time)

    def scrape_chalmers(self, time=None):
        self.scrape_affiliation('chalmers.se', 'Chalmers University of Technology', 'chalmers', time)

    def scrape_user(self, shortname, time=None):
        """Per-user backstop: scrape /users/<slug> for current global rank+score."""
        html = self.download_html(f"https://open.kattis.com/users/{shortname}")
        try:
            display_name, rank, score = parse_user_page(html)
        except ValueError as e:
            raise RuntimeError(f"could not parse user page for {shortname}: {e}") from e
        self.kattis_conn.add_user_backstop(shortname, display_name, rank, score, self._ts(time))

    def scrape(self, time=None):
        """Run all per-URL handlers in sequence. Used by tests and one-shot runs."""
        self.scrape_global_affiliations(time)
        self.scrape_global_users(time)
        self.scrape_global_countries(time)
        self.scrape_global_languages(time)
        self.scrape_swe(time)
        self.scrape_chalmers(time)
