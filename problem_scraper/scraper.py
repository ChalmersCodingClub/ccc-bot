import html as htmllib
import json
import re

from db.kattis_db_conn import ScrapeRejected, shortname_from_href
from scraper.http_client import EntityGone, KattisHttpClient


# DB `kind` -> Kattis HTML id stem (`toplist_<stem>_0`). Kattis renamed the
# stems (were the same as the kinds) ~2026-09-29; the DB kinds stay unchanged.
_TOPLIST_KINDS    = {'best_scoring': 'score', 'fastest': 'runtime', 'shortest': 'length'}
_LISTING_LINK_RE  = re.compile(r'<a href="/problems/([a-z0-9]+)"\s*>([^<]+)</a>')
_H1_RE            = re.compile(r'<h1>\s*(.+?)\s*[—-]\s*Problem Statistics', re.S)
_DIFFICULTY_RE    = re.compile(r'difficulty_number-problem_view[^"]*">\s*([0-9.]+)(?:\s*-\s*([0-9.]+))?')
_BREAKPOINTS_RE   = re.compile(r'data-breakpoints="([^"]*)"')
_DONUT_RE         = re.compile(r'id="status-donut-data"[^>]*>\s*(\{.*?\})\s*</script>', re.S)
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

        toplists, rejected = {}, {}
        for kind, stem in _TOPLIST_KINDS.items():
            rows, bad = self._parse_toplist(html, stem)
            if rows:
                toplists[kind] = rows
            if bad:
                rejected[kind] = (bad, len(rows) + len(bad))

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
            # kind -> ([bad href, ...], total rows): toplist rows whose user
            # link isn't a /users/<shortname> link. See scrape_problem.
            'rejected': rejected,
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

    def _parse_toplist(self, html, stem):
        idx = html.find(f'id="toplist_{stem}_0"')
        if idx < 0:
            return [], []
        # Only scan within the _0 block; per-language sections follow it and
        # must not bleed in when the all-languages table is absent (time-tie).
        next_block = html.find(f'id="toplist_{stem}_', idx + 1)
        end = next_block if next_block >= 0 else len(html)
        tables = self.get_tables(html[idx:end])
        if not tables:
            return [], []
        out, bad = [], []
        for r in tables[0]:
            if len(r) < 5:
                continue
            rank = int(r[0][0].replace(',', ''))
            user_text, user_slug = r[1]
            user_sn = shortname_from_href('user', user_slug)
            # Kattis anonymizes some users: no link at all, text "Hidden user".
            # They're kept with a NULL shortname; any other non-user link is bad.
            if user_sn is None and not (user_slug is None and user_text == 'Hidden user'):
                bad.append(user_slug)
                continue
            out.append((rank, user_sn, user_text, _value(r[2][0]), r[3][0], r[4][0]))
        return out, bad

    def scrape_problem(self, shortname, time=None):
        """Scrape one problem's statistics page and write all problem_* rows.
        HTTP 404 -> EntityGone (problem retired)."""
        html = self.download_html(f"https://open.kattis.com/problems/{shortname}/statistics")
        ts = self._ts(time)
        p = self.parse_problem(html)
        if p['rejected']:
            # Write nothing for this problem: a toplist missing rows (or written
            # empty) would read as a real ranking / a tie. It stays stale and is
            # retried next rotation. Same log prefixes as the ranklist writes.
            for kind, (bad, total) in p['rejected'].items():
                for href in bad:
                    print(f'REJECT problem_toplist ctx={shortname}/{kind} href={href!r}', flush=True)
                if len(bad) == total:
                    print(f'REJECT-ALL problem_toplist ctx={shortname}/{kind} rows={total}', flush=True)
            raise ScrapeRejected(f'{shortname}: toplist rows with bad user links: '
                                 + ', '.join(f'{k} {len(b)}/{t}' for k, (b, t) in p['rejected'].items()))
        # One transaction: a problem_obs row committed without its toplists
        # would read as "no toplist = tied / no holder".
        with self.kattis_conn.transaction():
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
