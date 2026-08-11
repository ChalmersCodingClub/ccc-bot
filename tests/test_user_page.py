"""Offline tests for the /users/<slug> profile-page parser.

Pure-function tests for `scraper.scraper.parse_user_page` — no DB, no network.
Run standalone: `python3 tests/test_user_page.py` (no pytest dependency).

These lock in the fix for the 2026-07-30 breakage: Kattis started emitting
whitespace between the `info_label` and `important_text` spans, which the
adjacency-anchored regexes required to be absent. Every per-user backstop
scrape failed (`rank=False score=False name=True`) for 12 days, and because
dynamic-job failures never `sys.exit`, nothing alerted.

CURRENT_MARKUP below is copied verbatim from a live fetch of /users/zozs on
2026-08-11 — keep it that way, whitespace included.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.scraper import parse_user_page


# Live markup, 2026-08-11. Note the newline+indent between each label span and
# its value span — that is exactly what broke the old regexes.
CURRENT_MARKUP = '''
<div class="image_info-text-horizontal">
  <a href="/users/zozs">
    <span class="image_info-text-main image_info-text-main-header pr-1">
      Linus Karlsson
    </span>
  </a>
</div>
<div class="divider_list-item divider_list-item-first">
  <span class="info_label">Rank</span>
  <span class="important_text">2508</span>
</div>
<div class="divider_list-item">
  <span class="info_label">Score</span>
  <span class="important_text">406.1</span>
</div>
'''

# Pre-2026-07-30 markup: spans directly adjacent. Must still parse, so a
# revert/CDN-cache of the old template doesn't break us the other way.
LEGACY_MARKUP = (
    '<span class="image_info-text-main image_info-text-main-header pr-1">Linus Karlsson</span>'
    '<span class="info_label">Rank</span><span class="important_text">2508</span>'
    '<span class="info_label">Score</span><span class="important_text">406.1</span>'
)


def test_current_markup():
    """The whitespace-separated markup Kattis serves today parses."""
    assert parse_user_page(CURRENT_MARKUP) == ('Linus Karlsson', 2508, 406.1)


def test_legacy_markup_still_parses():
    assert parse_user_page(LEGACY_MARKUP) == ('Linus Karlsson', 2508, 406.1)


def test_thousands_separator():
    """Scores/ranks use thousands-separator commas: "9,509.5" -> 9509.5."""
    html = CURRENT_MARKUP.replace('>2508<', '>10,432<').replace('>406.1<', '>9,509.5<')
    assert parse_user_page(html) == ('Linus Karlsson', 10432, 9509.5)


def test_unranked_user_is_none_not_error():
    """A user with no ranked submissions shows "-" -> None, and must NOT raise:
    the observation has to be written or the user stays permanently due."""
    html = CURRENT_MARKUP.replace('>2508<', '>-<').replace('>406.1<', '>-<')
    assert parse_user_page(html) == ('Linus Karlsson', None, None)


def test_missing_fields_raise_with_flags():
    """Failure names which fields matched — that detail is what made the
    2026-07-30 breakage diagnosable from the journal alone."""
    html = CURRENT_MARKUP.replace('info_label">Rank', 'info_label">Ranking')
    try:
        parse_user_page(html)
    except ValueError as e:
        assert str(e) == 'rank=False score=True name=True', str(e)
    else:
        assert False, "expected ValueError"


def test_empty_page_raises():
    try:
        parse_user_page('')
    except ValueError as e:
        assert str(e) == 'rank=False score=False name=False', str(e)
    else:
        assert False, "expected ValueError"


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"\n{len(tests)} passed")
