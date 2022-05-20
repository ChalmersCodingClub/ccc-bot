from urllib.request import Request, urlopen
from datetime import datetime
import db


class Scraper:
    def __init__(self):
        self.kattis_conn = db.KattisDbConn("db/kattis.db")

    def download_html(self, url):
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read().decode('utf-8')
        return webpage

    def parse_cell(self, cell):
        r = []
        cnt = 0
        for c in cell:
            if(c == '<'): cnt += 1
            if(cnt == 0): r.append(c)
            if(c == '>'): cnt -= 1
        r = "".join(r).strip()
        r = r.replace('\n', '')
        while(1):
            r2 = r.replace('  ', ' ')
            if(r2 == r): break
            r = r2
        return r

    def get_tables(self, webpage):
        tables = []
        i = 0
        while(i < len(webpage)):
            if(webpage.startswith('<table class="table table-hover table-kattis', i)):
                while(not webpage.startswith('<tbody>', i)): i+=1
                table = []
                while(1):
                    while(not webpage.startswith('<tr', i) and not webpage.startswith('</tbody>', i)): i+=1
                    if(webpage.startswith('</tbody>', i)): break
                    row = []
                    while(1):
                        while(not webpage.startswith('<td', i) and not webpage.startswith('</tr>', i)): i+=1
                        if(webpage.startswith('</tr>', i)): break
                        while(webpage[i] != '>'): i+=1
                        i += 1
                        cell = []
                        while(not webpage.startswith('</td>', i)):
                            cell.append(webpage[i])
                            i += 1
                        cell = self.parse_cell("".join(cell))
                        row.append(cell)
                    table.append(row)
                tables.append(table)
            i += 1
        return tables

    def download_tables(self, url):
        return self.get_tables(self.download_html(url))

    def scrape(self, time=None):
        global_uni = self.download_tables("https://open.kattis.com/ranklist/universities")[0]
        global_user = self.download_tables("https://open.kattis.com/ranklist")[0]
        global_country = self.download_tables("https://open.kattis.com/ranklist/countries")[0]

        swe_tables = self.download_tables("https://open.kattis.com/countries/SWE")

        chalmers_user = self.download_tables("https://open.kattis.com/universities/chalmers.se")[0]
        for r in chalmers_user:
            r.insert(3, "Chalmers University of Technology")

        self.kattis_conn.add_data(global_uni, global_user, global_country, swe_tables, chalmers_user, time)

