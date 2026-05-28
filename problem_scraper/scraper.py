import html as htmllib
import json
import re

from scraper.http_client import EntityGone, KattisHttpClient


_TOPLIST_KINDS    = ('best_scoring', 'fastest', 'shortest')
_LISTING_LINK_RE  = re.compile(r'<a href="/problems/([a-z0-9]+)"\s*>([^<]+)</a>')
_H1_RE            = re.compile(r'<h1>\s*(.+?)\s*[—-]\s*Problem Statistics', re.S)
_DIFFICULTY_RE    = re.compile(r'difficulty_number-problem_view[^"]*">\s*([0-9.]+)(?:\s*-\s*([0-9.]+))?')
_BREAKPOINTS_RE   = re.compile(r'data-breakpoints="([^"]*)"')
_DONUT_RE         = re.compile(r'id="status-donut-data"[^>]*>\s*(\{.*?\})\s*</script>', re.S)
_MEGATIE_RE       = re.compile(r'data-title="\d+ users have solved this problem with a score of \d+ \(all languages\)"')
_SPAN_RE          = re.compile(r'<span>([^<]+)</span>')


def _value(text):
    """Toplist value: score (100), runtime ('0.01 s'), or byte length (555)."""
    return float(text.replace(',', '').replace(' s', '').strip())


class ProblemScraper(KattisHttpClient):
    """Scrapes /problems listing (discovery) and /problems/<slug>/statistics
    (observations). Writes the problem_* tables via KattisDbConn."""

    # ---- discovery ----------------------------------------------------------

    def scrape_problem_list(self, page):
        """Return [(shortname, display_name), ...] for one /problems page.
        An empty list signals we've gone past the last page."""
        html = self.download_html(f"https://open.kattis.com/problems?page={page}")
        return [(slug, htmllib.unescape(name.strip())) for slug, name in _LISTING_LINK_RE.findall(html)]

    # ---- per-problem statistics ---------------------------------------------

    def parse_problem(self, html):
        """Parse a /problems/<slug>/statistics page into a dict. Pure function
        of the HTML (no network/DB) so it's unit-testable offline."""
        name_m = _H1_RE.search(html)
        display_name = htmllib.unescape(name_m.group(1).strip()) if name_m else None

        diff_m = _DIFFICULTY_RE.search(html)
        low = float(diff_m.group(1)) if diff_m else None
        high = float(diff_m.group(2)) if (diff_m and diff_m.group(2)) else low

        scalars = self._parse_scalars(html)

        breakpoints = []
        bp_m = _BREAKPOINTS_RE.search(html)
        if bp_m:
            data = json.loads(htmllib.unescape(bp_m.group(1)))
            breakpoints = [(b['breakpoint'], b['difficulty']) for b in data]

        verdicts = []
        donut_m = _DONUT_RE.search(html)
        if donut_m:
            d = json.loads(donut_m.group(1))
            verdicts = [(label, int(count)) for label, count in zip(d['labels'], d['data'])]

        megatie = bool(_MEGATIE_RE.search(html))
        toplists = {}
        for kind in _TOPLIST_KINDS:
            if kind == 'best_scoring' and megatie:
                continue  # full-solve tie — uninteresting, skip
            rows = self._parse_toplist(html, kind)
            if rows:
                toplists[kind] = rows

        return {
            'display_name': display_name,
            'difficulty_low': low,
            'difficulty_high': high,
            'submissions': scalars['submissions'],
            'accepted': scalars['accepted'],
            'authors': scalars['authors'],
            'full_solves': scalars['full_solves'],
            'breakpoints': breakpoints,
            'verdicts': verdicts,
            'toplists': toplists,
            'megatie': megatie,
        }

    def _parse_scalars(self, html):
        i = html.find('table2 condensed')
        end = html.find('</table>', i)
        block = html[i:end] if i >= 0 and end >= 0 else html

        def g(label):
            j = block.find('>' + label)
            if j < 0:
                return None
            m = _SPAN_RE.search(block, j)
            return int(m.group(1).replace(',', '')) if m else None

        return {
            'submissions': g('Submissions'),
            'accepted':    g('Accepted submissions'),
            'authors':     g('Authors'),
            'full_solves': g('Full solves'),
        }

    def _parse_toplist(self, html, kind):
        idx = html.find(f'id="toplist_{kind}_0"')
        if idx < 0:
            return []
        tables = self.get_tables(html[idx:])
        if not tables:
            return []
        out = []
        for r in tables[0]:
            if len(r) < 5:
                continue
            rank = int(r[0][0].replace(',', ''))
            user_text, user_slug = r[1]
            user_sn = user_slug.removeprefix('users/') if user_slug else None
            out.append((rank, user_sn, user_text, _value(r[2][0]), r[3][0], r[4][0]))
        return out

    def scrape_problem(self, shortname, time=None):
        """Scrape one problem's statistics page and write all problem_* rows.
        HTTP 404 -> EntityGone (problem retired)."""
        html = self.download_html(f"https://open.kattis.com/problems/{shortname}/statistics")
        ts = self._ts(time)
        p = self.parse_problem(html)
        self.kattis_conn.add_problem_obs(
            shortname, p['display_name'], ts,
            p['difficulty_low'], p['difficulty_high'],
            p['submissions'], p['accepted'], p['authors'], p['full_solves'],
        )
        if p['verdicts']:
            self.kattis_conn.add_problem_verdicts(shortname, ts, p['verdicts'])
        if p['breakpoints']:
            self.kattis_conn.add_problem_partial_difficulty(shortname, ts, p['breakpoints'])
        for kind, rows in p['toplists'].items():
            self.kattis_conn.add_problem_toplist(shortname, ts, kind, rows)
