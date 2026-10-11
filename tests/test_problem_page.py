"""Offline tests for the /problems/<slug>/statistics toplist parser.

Pure-function tests for `ProblemScraper.parse_problem` — no DB, no network.
Run standalone: `python3 tests/test_problem_page.py` (no pytest dependency).

These lock in the fix for the ~2026-09-29 breakage: Kattis renamed the toplist
block ids `toplist_{best_scoring,fastest,shortest}_N` to
`toplist_{score,runtime,length}_N`. The parser found no `_0` block, returned
`[]` for every kind, and — since an empty toplist is a legal state (ties) — no
error surfaced; `problem_toplist` simply stopped growing for 11 days.

MARKUP is trimmed live markup (2026-10-11): the score and length `_0` blocks
from /problems/wall/statistics (rows cut), and the empty (tied) runtime `_0`
block from /problems/hello/statistics followed by its first per-language
section, which must NOT bleed into the all-languages list.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from problem_scraper.scraper import ProblemScraper


MARKUP = '''<div id="toplist_score_0" class="strip strip-item-plain" style="display:none">

    <div class="strip strip-item-plain">
        <div class="table-wrapper">
             



                <table class="table2 ">
                    <thead>
    <tr>
                <th class="  " data-name="rank">
                            #
                </th>
                <th class="  " data-name="user_link">
                            User
                </th>
                <th class="  " data-name="score_disp">
                            Score
                </th>
                <th class="  " data-name="language_name">
                            Language
                </th>
                <th class="  " data-name="time">
                            Date
                </th>
                <th class="  " data-name="submission_link">
                            
                </th>
    </tr>
                    </thead>
                    <tbody>
        <tr class="" >

                        <td class="  ">1</td>

                        <td class="  ">
                            <a href="/users/kristjan-lepp" title="Kristjan Lepp">Kristjan Lepp</a>

</td>

                        <td class="  ">100</td>

                        <td class="  ">C++</td>

                        <td class="  ">            2024-09-26 16:58:36
</td>

                        <td class="  ">
</td>
        </tr>
        <tr class="" >

                        <td class="  ">2</td>

                        <td class="  ">
                            <a href="/users/jeremy-lim" title="Jeremy Lim">Jeremy Lim</a>

</td>

                        <td class="  ">100</td>

                        <td class="  ">C++</td>

                        <td class="  ">            2024-10-07 12:46:42
</td>

                        <td class="  ">
</td>
        </tr>
        </tbody>
                </table>
        </div>
    </div>
    </div>
<div id="toplist_runtime_0" class="strip strip-item-plain" style="display:none">
    </div>

            <section id="toplist_runtime_52" class="strip strip-item-plain" style="display:none">

    <div class="strip strip-item-plain">
        <div class="table-wrapper">
             



                <table class="table2 ">
                    <thead>
    <tr>
                <th class="  " data-name="rank">
                            #
                </th>
                <th class="  " data-name="user_link">
                            User
                </th>
                <th class="  " data-name="runtime_disp">
                            Runtime
                </th>
                <th class="  " data-name="language_name">
                            Language
                </th>
                <th class="  " data-name="time">
                            Date
                </th>
                <th class="  " data-name="submission_link">
                            
                </th>
    </tr>
                    </thead>
                    <tbody>
        <tr class="" >

                        <td class="  ">1</td>

                        <td class="  ">
                            <a href="/users/i-pwn" title="Bjørn Chr. Soltvedt">Bjørn Chr. Soltvedt</a>

</td>

                        <td class="  ">0.00 s</td>

                        <td class="  ">Ada</td>

                        <td class="  ">            2024-06-18 07:34:38
</td>

                        <td class="  ">
</td>
        </tr>
        </tbody>
                </table>
        </div>
    </div>
</section>
<div id="toplist_length_0" class="strip strip-item-plain" style="display:none">

    <div class="strip strip-item-plain">
        <div class="table-wrapper">
             



                <table class="table2 ">
                    <thead>
    <tr>
                <th class="  " data-name="rank">
                            #
                </th>
                <th class="  " data-name="user_link">
                            User
                </th>
                <th class="  " data-name="length_disp">
                            Length
                </th>
                <th class="  " data-name="language_name">
                            Language
                </th>
                <th class="  " data-name="time">
                            Date
                </th>
                <th class="  " data-name="submission_link">
                            
                </th>
    </tr>
                    </thead>
                    <tbody>
        <tr class="" >

                        <td class="  ">1</td>

                        <td class="  ">
                            <a href="/users/jeremy-lim" title="Jeremy Lim">Jeremy Lim</a>

</td>

                        <td class="  ">2028</td>

                        <td class="  ">C++</td>

                        <td class="  ">            2024-10-09 18:58:36
</td>

                        <td class="  ">
</td>
        </tr>
        </tbody>
                </table>
        </div>
    </div>
    </div>
'''


def _toplists(html):
    # parse_problem is pure; skip __init__ (it opens the DB).
    return ProblemScraper.__new__(ProblemScraper).parse_problem(html)['toplists']


def test_current_ids_parse():
    t = _toplists(MARKUP)
    assert t['best_scoring'] == [
        (1, 'kristjan-lepp', 'Kristjan Lepp', 100.0, 'C++', '2024-09-26 16:58:36'),
        (2, 'jeremy-lim', 'Jeremy Lim', 100.0, 'C++', '2024-10-07 12:46:42'),
    ], t.get('best_scoring')
    assert t['shortest'] == [
        (1, 'jeremy-lim', 'Jeremy Lim', 2028.0, 'C++', '2024-10-09 18:58:36'),
    ], t.get('shortest')


def test_empty_all_languages_block_does_not_bleed():
    """Tied runtime list: `_0` is empty; the per-language section after it
    (Ada, rank 1 = i-pwn) must not be taken as the all-languages list."""
    assert 'fastest' not in _toplists(MARKUP)


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for t in tests:
        t()
        print(f'ok  {t.__name__}')
    print(f'\n{len(tests)} passed')


if __name__ == '__main__':
    _run_all()
