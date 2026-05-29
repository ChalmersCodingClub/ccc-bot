# CLAUDE.md — ccc-bot

A Discord bot that tracks Kattis (competitive-programming judge) ranklist
standings over time and plots score/rank history graphs. "CCC" = Chalmers
Coding Club; the bot is biased toward Chalmers / Swedish
users but tracks global data too.

## Architecture: three systemd services, one shared SQLite DB

- **Bot** (`main.py`, `services/cccbot.service`, `services/start.sh`) —
  `python main.py`. Discord-facing. Reads `db/kattis.db`, owns `db/user.db`.
  Never scrapes.
- **Ranklist scraper** (`scraper/__main__.py`, `services/cccbot-scraper.service`,
  `services/start-scraper.sh`) — `python -m scraper`. Polls Kattis ranklists,
  writes `db/kattis.db`. No Discord dependency.
- **Problem scraper** (`problem_scraper/__main__.py`,
  `services/cccbot-problem-scraper.service`, `services/start-problems.sh`) —
  `python -m problem_scraper`. Polls per-problem statistics pages, writes the
  `problem_*` tables in `db/kattis.db`. Separate service so problem-scrape
  scheduling is decoupled from the ranklist loop. See "Problem statistics".

Both scrapers reuse the HTTP + HTML-table primitives in
`scraper/http_client.py` (`KattisHttpClient`: `download_html`, `get_tables`,
`parse_cell`, `EntityGone`). `Scraper` and `ProblemScraper` both subclass it.

They share only the SQLite files. SQLite's file lock serializes the single
writer (scraper) against readers (bot); the bot also writes via `/track-user`
(see below), but each thread/process uses its own connection, and
`KattisDbConn` opens with `check_same_thread=False` so the bot can scrape from
an `asyncio.to_thread` worker.

Splitting the scraper out of the bot was deliberate: scrape scheduling
shouldn't be coupled to the Discord event loop or bot reconnects.

## Database (`db/kattis.db`)

Schema managed by `db/kattis_db_conn.py:KattisDbConn`.

**Observation tables** — one per entity kind, all time-series:
- `user_obs(timestamp, context, shortname, display_name, rank, score, place, affiliation)`
- `affiliation_obs(timestamp, context, shortname, display_name, rank, score, subdiv, num_users)`
- `country_obs(timestamp, context, shortname, display_name, rank, score, num_users, num_affiliations)`
- `subdivision_obs(timestamp, context, shortname, display_name, rank, score, country)`
- `language_obs(timestamp, context, shortname, display_name, rank, score, num_users)`

`context` ∈ `{'global', 'swe', 'chalmers', <affiliation-slug>, <country-slug>}`.
The same user appears once per context per scrape; **score is identical across
contexts, only rank differs** (rank is position within that ranklist).

**Metadata table:**
- `entities(kind, shortname, display_name, tracked, discover_users, discover_affiliations, first_seen, last_seen_alive)` — PK `(kind, shortname)`.
  - `tracked` — sticky boolean, "I care about this entity." Set by: observation
    in a `context='global'` scrape, OR discovery scrape (`force_tracked`), OR
    manual `set_flags`. Consumed by the scraper to decide what to backstop.
    The qualifier is "I care", not literally top-100.
  - `discover_users` / `discover_affiliations` — flags that make the scraper
    enumerate an entity's sub-entities. Set manually via `set_flags`.
  - `last_seen_alive` — last successful observation. Drives 10-day decay.

### Problem statistics tables

Written by the **problem scraper** (see below). All time-series, no `context`
column (problems aren't per-ranklist). Created by `create_tables`
(`CREATE TABLE IF NOT EXISTS`) — no migration needed.
- `problem_obs(timestamp, shortname, display_name, difficulty_low,
  difficulty_high, submissions, accepted, authors, full_solves)` —
  `difficulty_low==difficulty_high` for binary (non-partial) problems.
- `problem_verdict(timestamp, shortname, verdict, count)` — one row per
  verdict-donut slice. Long table (not fixed columns) because the slice set
  varies per problem (Accepted/WA/TLE/RTE/CE plus e.g. `Other`, Memory Limit
  Exceeded, Judge Error).
- `problem_partial_difficulty(timestamp, shortname, breakpoint, difficulty)` —
  one row per partial-score breakpoint. Empty for binary problems.
- `problem_toplist(timestamp, shortname, kind, rank, user_shortname,
  user_display_name, value, language, solved_at)` — `kind` ∈
  `{best_scoring, fastest, shortest}`, **all-languages lists only**. `value`
  is score / runtime-seconds / byte-length by kind.

Problems are `entities` rows of `kind='problem'`, all `tracked=1`.

**Intentionally NOT stored — inferable from the above** (don't re-add): the
`submission_ratio` (= accepted/submissions), `full_solve_ratio`
(= full_solves/authors), and `difficulty_category` (Easy/Medium/Hard, derives
from the difficulty number). The "Solution running time distribution" on the
stats page is also not scraped (by request — uninteresting).

### shortname (slug) handling — IMPORTANT
- Slugs are Kattis URL identifiers: user `joshua-andersson`, affiliation
  `chalmers.se`, country `SWE` (ISO-3166-α3), subdivision `SWE/AB` (ISO-3166-2).
  They are **opaque** — `jasnah` = "Alexander Skidanov". Display name ≠ slug.
- Slugs are captured **going forward** from the first `<a href>` in each
  scraped cell. **Historical rows have `shortname=NULL`** — they predate
  capture. A one-shot backfill from a Kattis admin DB dump is planned but not
  done; do not assume historical rows have slugs.
- Languages have no anchor/slug → `shortname = display_name` for them.
- **The bot still queries by `display_name`, not slug** (see `history()`),
  because that's what users type and what historical rows have. Migrating the
  read path to slug-keyed identity waits for the Kattis dump.

## Scraper (`scraper/scraper.py` + `scraper/__main__.py`)

`Scraper` does HTTP + parsing + DB writes. `__main__` is the scheduling loop.

### Parsing
- `get_tables(html)` — hand-rolled HTML table parser keyed on
  `class="table2 "`. Returns rows of `(text, slug)` cell tuples.
  `parse_cell` strips tags and extracts the first `<a href>` slug.
- Per-URL handlers: `scrape_global_users/affiliations/countries/languages`,
  `scrape_country(slug, context)`, `scrape_affiliation(slug, name, context)`,
  `scrape_user(slug)` (backstop). `scrape_swe`/`scrape_chalmers` are aliases.
- `scrape_user` parses the user *profile* page (not a table) via regex for
  rank/score/display_name. HTTP 404 → raises `EntityGone`.

### Scheduling loop (`__main__.py`)
Each tick rebuilds the job list from `entities`:
- **4 fixed jobs**: global ranklists (users, affiliations, countries,
  languages). Always present.
- **Discovery jobs**: for each `tracked` entity with `discover_users` or
  `discover_affiliations` set and alive (`last_seen_alive` within 10 days) —
  scrape its page, enumerate sub-entities, mark them `tracked` (`force_tracked`).
- **Per-user backstop jobs**: for each `tracked` alive user — scrape
  `/users/<slug>` to capture global rank/score. Only *fires* (is "due") when
  that user's `user_obs context='global'` is >24h stale, so users in
  `/ranklist` top-100 are skipped (the global job already covers them).

A job is "due" when its target table's latest matching timestamp is >24h old.
Each tick runs the first due job, then sleeps an **adaptive interval**:
`max(30, min(600, 86400 // n_due))` — fast enough to cover everyone within 24h,
30s polite floor, 600s ceiling when little is due.

**Failure handling**: fixed jobs use a per-job counter and `sys.exit(1)` after
10 consecutive fails (systemd restarts → loud signal for infra breakage).
Dynamic jobs: `EntityGone` (404) is logged and does *not* bump
`last_seen_alive` (entity decays after 10 silent days); other transient errors
are logged and retried next tick. One bad entity never takes down the scraper.

### Adding tracking targets
Manual, via `KattisDbConn.set_flags(kind, shortname, **flags)`. Example:
```python
from db import KattisDbConn
c = KattisDbConn('db/kattis.db')
c.set_flags('affiliation', 'kth.se', tracked=1, discover_users=1)
```
The next scraper tick picks it up — no restart, no code change.

## Problem scraper (`problem_scraper/`)

`ProblemScraper` (`problem_scraper/scraper.py`, subclasses `KattisHttpClient`)
+ a rotation loop in `problem_scraper/__main__.py`. Scrapes **all** ~6,000
Kattis problems as full time-series.

### Parsing (`/problems/<slug>/statistics`)
`parse_problem(html)` is a pure function (offline-testable) returning scalars,
difficulty range, partial-score breakpoints (from the `data-breakpoints`
canvas attr — HTML-entity-encoded JSON), the verdict donut (from the
`status-donut-data` `<script>` JSON), and the three all-languages toplists
(the `toplist_<kind>_0` tables). The condensed summary table
(`class="table2 condensed …"`) is parsed by label, not `get_tables` (its class
isn't exactly `table2 `). `scrape_problem_list(page)` parses the `/problems`
listing for `(slug, name)` pairs (name links are `/problems/<slug>` with no
sub-path; statistics/language links have sub-paths and are ignored).

### Mega-tie skip
When a problem's best_scoring "All languages" list is a full-solve tie, Kattis
emits `data-title="N users have solved this problem with a score of 100 (all
languages)"`. We detect this and **skip the best_scoring toplist** (it's
uninteresting); fastest/shortest are still stored. Binary problems have no
best_scoring toplist at all, so it's naturally absent there too.

### Rotation loop (`__main__.py`)
1. **Discovery** (once per rotation): paginate `/problems?page=N` until a page
   is empty, `register_problems` each page (marks every problem `tracked=1`
   and refreshes `last_seen_alive`). Used for discovery only — listing scalars
   are not stored (partial info, messy).
2. **Observation**: snapshot `problems_to_scrape(alive_since)` (all tracked,
   alive problems, **stalest-first**) and scrape each detail page, sleeping
   `POLITE_INTERVAL_SECONDS` (60s) between. Snapshotting once per rotation (vs
   re-picking the stalest each step) avoids getting stuck re-fetching a problem
   whose scrape failed. At ~60s/problem a full rotation ≈ 4–6 days. Stalest-
   first ordering keeps coverage even across restarts.

**Failure handling**: list-discovery failures use a consecutive-fail counter →
`sys.exit(1)` after 10 (systemd restart), like the ranklist scraper's fixed
jobs. Per-problem `EntityGone` (404, retired) is logged and doesn't bump
`last_seen_alive` (decays after 10 days off the listing); other per-problem
errors are logged and the rotation continues.

## Bot (`main.py`)

Prefix `$`, plus one slash command. Commands:
- `$kattis [user|uni|country] [names...] [top=...] [score|rank|...] [global|swe|chalmers] [days=N] [log] [nozoom] [legend|nolegend] [ignore-not-found]`
  — the main graphing command. Parses a loose grammar into matplotlib plots.
  Calls `kattis_conn.history()` and `get_top()`.
- `$setname` / `$whoami` / `$forgetme` — map a Discord ID to a Kattis display
  name (`db/user.db`, `realname` table). `$kattis me` / `@mention` resolve through it.
- `/track-user shortname:<slug>` — slash command. Validates the user exists on
  Kattis (`scrape_user` → `EntityGone` on 404), sets `tracked=1`, writes an
  initial observation. Ephemeral responses. Slash commands are guild-synced in
  `on_ready` (guarded by `_synced` flag against reconnects).

`history(mintimestamp, type, names, place)` returns rows in the legacy
6-tuple shape `(timestamp, rank, display_name, place/subdiv/num, affiliation/num, score)`
so `main.py`'s unpacking is unchanged. When `place='all'` it merges contexts
and dedups observations within 3600s (collapses the per-context duplicates).

## Kattis HTML quirks (learned the hard way)
- Tables are `<table class="table2 ">` (trailing space). They previously had
  `report_grid-problems_table` classes — Kattis changed it; the parser was
  updated. If scraping breaks with "list index out of range", check the class
  string first.
- Scores use thousands-separator commas (`"9,509.5"`). `_num()` strips them
  before `int`/`float`.
- Per-affiliation pages show **top 50**; global ranklists show **top 100**;
  languages ~55 (all of them). No pagination available — we take what one page
  gives.
- User profile pages have **no historical data** — only a current snapshot.
  The DB is the only history; there's no way to backfill past rank/score.
- `/ranklist/teams` is 404 (no public team ranklist). `/ranklist/challenge`
  exists (a separate user score) but is intentionally **not** scraped.

## Deployment
**Production deploys are run by the user, not by Claude.** Surface the command
list (stop services → backup `db/kattis.db` → `git pull` →
install/enable any **new** service unit, e.g. `cccbot-problem-scraper.service`
via `systemctl daemon-reload` + `enable --now` → start services → tail
`journalctl`); the user executes on the deploy host
(`webmaster@po:/home/webmaster/ccc-bot`). A production-DB copy for local
verification lives at `~/temp/cccbot-backup`. Verify against it before handing
off deploy commands.

## Deferred / future work
- **Per-affiliation/country aggregate backstop** — if an affiliation drops out
  of `/ranklist/affiliations` top 100 we lose its aggregate score series. Only
  per-*user* backstop exists today.
- **Slug-keyed bot reads** — migrate `history`/`get_top` and the `realname`
  table off display-name keying once the Kattis admin slug-dump lands. This
  fixes rename-breakage and duplicate-display-name conflation.
- **Bot UX** — `/track-user` exists; a fuller tracking UI (list/untrack, track
  by mention) could follow.
- **Problem graphing in the bot** — the problem scraper stores `problem_*`
  time-series, but the bot has no read path / command to plot them yet
  (difficulty/submissions/verdict trends, toplists). To add later.
- **Known latent bugs** (from an earlier review, not yet fixed; out of scope of
  the rewrite): `$kattis` crashes on `arg.split('=')` with multiple `=`, on
  non-numeric `top=`/`days=`, on empty quoted args; `<@!` mention prefix misses
  modern `<@` mentions; empty `names` could hit `IN ()`. Harden with try/except
  if revisiting the command parser.
