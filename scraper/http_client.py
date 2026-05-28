import re
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import db


_HREF_RE = re.compile(r'<a [^>]*href="([^"]+)"')


class EntityGone(Exception):
    """Raised when a per-entity scrape hits HTTP 404 — the entity no longer
    exists on Kattis and should not have its last_seen_alive bumped."""


class KattisHttpClient:
    """Low-level HTTP + HTML-table parsing shared by every Kattis scraper.

    Owns a KattisDbConn. Subclasses (Scraper, ProblemScraper) add the
    page-specific scrape methods on top of these primitives."""

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

    def get_tables(self, webpage, table_class='table2 '):
        marker = f' class="{table_class}"'
        tables = []
        i = 0
        while i < len(webpage):
            if webpage.startswith(marker, i):
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

    def download_tables(self, url, table_class='table2 '):
        return self.get_tables(self.download_html(url), table_class)

    @staticmethod
    def _ts(time):
        if time is None: time = datetime.now()
        return int(time.timestamp())
