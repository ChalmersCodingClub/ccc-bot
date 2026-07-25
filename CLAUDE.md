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
- `entities(kind, shortname, display_name, tracked, discover_users, discover_affiliations, observe_users, observe_affiliations, first_seen, last_seen_alive)` — PK `(kind, shortname)`.
  - `tracked` — sticky boolean, "I care about this entity." Set by: observation
    in a `context='global'` scrape, OR discovery scrape (`force_tracked`), OR
    manual `set_flags`. Consumed by the scraper to decide what to backstop.
    The qualifier is "I care", not literally top-100.
  - `discover_users` / `discover_affiliations` — flags that make the scraper
    enumerate an entity's sub-entities. Set manually via `set_flags`.
  - `observe_users` / `observe_affiliations` — "scrape this country's user
    toplist / affiliation (uni) aggregate list into `user_obs` / `affiliation_obs`
    (context = its slug), but **untracked**." Set manually via `set_flags`.
    Unlike `discover_*` they write rows with `force_tracked=False`, so the
    sub-entities do **not** become tracked and do **not** spawn per-user
    backstop jobs; the country observe path also never touches the subdivisions
    table (no discovery, and avoids the missing-`tables[2]` crash on
    subdivision-less countries). They are the no-backstop **alternative** to
    `discover_*` — set the observe flags or the discover flags on a given
    entity, not both. (`observe_users` also applies to an affiliation entity, to
    grab its user toplist untracked; `observe_affiliations` is country-only.)
    Added via a guarded `ALTER TABLE` migration in `create_tables`.
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
- **Observe jobs**: for each alive entity with `observe_users` and/or
  `observe_affiliations` set — one `country-observe/<slug>` (or
  `affiliation-users/<slug>`) job that scrapes the requested tables via the
  `track=False` path into `user_obs`/`affiliation_obs` (context = its slug),
  **without** tracking the sub-entities (no backstop fan-out) and without
  touching subdivisions. Due-checked against the context's `user_obs` (or
  `affiliation_obs` if only affiliations are observed). (Gated on the observe
  flags alone, independent of `tracked`.)
- **Per-user backstop jobs**: for each `tracked` alive user — scrape
  `/users/<slug>` to capture global rank/score. Only *fires* (is "due") when
  that user's `user_obs context='global'` is >24h stale, so users in
  `/ranklist` top-100 are skipped (the global job already covers them).

A job is "due" when its target table's latest matching timestamp is >24h old.
Each tick evaluates every job's due-check **once**, into a `{name: bool}` map
(`is_due`) reused for both the pick and the `n_due` count — one DB query per job
per tick, no second pass. Don't reintroduce a staleness `ORDER BY` in
`_build_dynamic_jobs`: it adds a correlated `MAX(timestamp)` scan per entity
(`user_obs` has no `shortname` index) and, worse, see the ordering note below.

`pick_job(jobs, is_due, last_dyn_name)` then runs **one** job:
- **Fixed jobs first**, in list order, if due.
- **Dynamic jobs round-robin by name** — sorted by their stable unique name,
  taking the first due name greater than `last_dyn_name` (the cursor), wrapping
  to the first when past the end. The cursor is advanced *before* the handler
  runs, so a job that throws is still stepped past.

Round-robin (not stalest-first) because a job whose due-check can never clear —
a 404 user, whose `EntityGone` means no observation is ever written — is
permanently due. Under first-due-wins it monopolizes every tick and starves
everything behind it, and a stalest-first order sorts exactly that job to the
front. Round-robin caps a wedged job at one tick per rotation. Tradeoff: a job
that comes due just *behind* the cursor waits for the wrap (≤ one rotation,
~24h) instead of being prioritized; acceptable at 24h backstop granularity.
⚠️ `last_dyn_name` is **in-memory only** — it resets to the start of the
alphabet on every restart, so a crash-looping scraper re-serves the front of the
list. Regression tests: `tests/test_scheduler.py` (pure, no DB/network; run
`python3 tests/test_scheduler.py`). The wedge test only has teeth if the wedged
job sorts *first* — it asserts that.

Then sleep an **adaptive interval**: `max(30, min(600, 86400 // n_due))` — fast
enough to cover everyone within 24h, 30s polite floor, 600s ceiling when little
is due. `n_due` comes from the pre-run snapshot, so the just-run job still
counts (interval marginally short; harmless).

**Failure handling**: fixed jobs use a per-job counter and `sys.exit(1)` after
10 consecutive fails (systemd restarts → loud signal for infra breakage).
Dynamic jobs: `EntityGone` (404) is logged and does *not* bump
`last_seen_alive` (entity decays after 10 silent days); other transient errors
are logged and retried next tick. One bad entity never takes down the scraper.

⚠️ The fixed-job counter only counts **exceptions**. A fixed scrape that
*succeeds* but writes fewer than `FIXED_SCRAPE_MIN_ROWS` rows prints `ok`, never
clears its own due-check, and — having absolute priority — takes every tick
forever with the counter untouched and no `sys.exit`. Fixed jobs are deliberately
exempt from the dynamic round-robin, so nothing else bounds this. Latent, not
observed.

### Adding tracking targets
Manual, via `KattisDbConn.set_flags(kind, shortname, **flags)`. Example:
```python
from db import KattisDbConn
c = KattisDbConn('db/kattis.db')
c.set_flags('affiliation', 'kth.se', tracked=1, discover_users=1)
```
The next scraper tick picks it up — no restart, no code change.

To collect a country's **user toplist and/or uni list without** tracking those
sub-entities or running discovery, use the observe flags instead — e.g. a
national ranklist + uni list for Iceland:
```python
c.set_flags('country', 'ISL', observe_users=1, observe_affiliations=1)
# → user_obs + affiliation_obs context='ISL', all untracked
```

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

### Empty toplists
Any of the three toplist kinds may be absent or empty: `best_scoring` is not
present at all on pass/fail problems; any kind's "All languages" (`_0`) block
is left empty by Kattis when there's a tie (score-tie or time-tie). The parser
scopes its table search to within the `_0` block, so absent/empty → `[]` with
no bleed into per-language sections that follow.

### Rotation loop (`__main__.py`)
1. **Discovery** (once per rotation): paginate `/problems?page=N` until a page
   is empty, `register_problems` each page (marks every problem `tracked=1`
   and refreshes `last_seen_alive`). Used for discovery only — listing scalars
   are not stored (partial info, messy).
2. **Observation**: snapshot `problems_to_scrape(alive_since)` (all tracked,
   alive problems, **stalest-first**) and scrape each detail page, sleeping
   `POLITE_INTERVAL_SECONDS` (30s) between. Snapshotting once per rotation (vs
   re-picking the stalest each step) avoids getting stuck re-fetching a problem
   whose scrape failed. At ~30s/problem a full rotation ≈ 2–3 days. Stalest-
   first ordering keeps coverage even across restarts.

**Failure handling**: list-discovery failures use a consecutive-fail counter →
`sys.exit(1)` after 10 (systemd restart), like the ranklist scraper's fixed
jobs. Per-problem `EntityGone` (404, retired) is logged and doesn't bump
`last_seen_alive` (decays after 10 days off the listing); other per-problem
errors are logged and the rotation continues.

## Bot (`main.py` + `kattis_cmd.py` + `plot.py`)

**Slash-only** (the legacy `$`-prefix commands were removed; `message_content`
intent is off). Three layers for the graphing command:
- `plot.py` — **pure** render layer. `Metric`/`Scope` enums, `PlotRequest`
  dataclass, `render(req, series) -> PNG bytes`. Uses the OO Matplotlib
  `Figure` API (NOT global `pyplot`) because rendering runs in
  `asyncio.to_thread`. Offline-testable. Raises `ValueError` on `log`+`nozoom`.
- `kattis_cmd.py` — the `/kattis` `app_commands.Group` with **subcommands by
  type**: `/kattis user|uni|country`. Each exposes only its valid `metric`
  choices (user: score/rank; uni: +num_users; country: +num_users/
  num_affiliations) so the metric×type matrix is structural, not runtime.
  Shared options (in display order): `names` (comma-separated, with autocomplete
  that completes the last token via `distinct_display_names`), `days`, `metric`,
  `scope`, `top`, `log`; `user` also has a native `member:` picker. All three
  subcommands delegate to `_run`. `setup(kattis_conn, user_conn)` wires the DB
  handles. Empty input → caller's `/setname` (user) else top-N **of the chosen
  scope** (not always global). The legend auto-shows for ≤10 lines
  (`plot._LEGEND_MAX_LINES`); no manual toggle.
  - **`scope` choices are restricted per type** to the ranklists that exist
    (`_allowed_contexts`): user g/swe/chalmers, uni g/swe, country none (global
    only). Offering a scope a type has no ranklist for would just yield empty
    results.
  - **`rank` never defaults to `all`** — it's a position within ONE ranklist
    and differs across contexts, so `_default_scope` returns a concrete scope
    for rank (chalmers for user, global else). `all` is fine for score/#users/
    #unis, which are context-invariant, and merges them via `history`'s dedup.
  Missing names → public "couldn't find: …" note alongside the graph; errors/
  personal config are ephemeral.
- `main.py` — wiring only. Registers the group, `/track-user`, and the
  ephemeral `/setname` `/whoami` `/forgetme`. **Global** `tree.sync()` in
  `on_ready` (guarded by `_synced`) so commands work in DMs as well as guilds
  (global publishes can take ~1h to propagate the first time).

`history(mintimestamp, type, names, place)` returns
`[(display_name, [HistoryRow, ...])]` where `HistoryRow` is a namedtuple
`(timestamp, rank, display_name, score, num_users, num_affiliations)` (N/A
fields are `None`), letting the plot loop pick a metric by attribute. Rows are
sorted by `.timestamp`. When `place='all'` it merges contexts and dedups
observations within 3600s (collapses the per-context duplicates).
`distinct_display_names(type, prefix, limit)` backs autocomplete; the `*_obs`
tables have a `display_name` index for it (and for the `history` `IN (...)`
scan).

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

⚠️ **`db/kattis.db` runs in WAL mode** (`KattisDbConn.__init__` sets
`PRAGMA journal_mode=WAL`), so committed data can live in a `db/kattis.db-wal`
sidecar (plus `-shm`) that a hot copy of the main file alone would miss. The
"backup `db/kattis.db`" step must therefore either **stop the services first**
(a clean shutdown checkpoints the WAL into the main file) or run
`PRAGMA wal_checkpoint(TRUNCATE)` before copying — otherwise copy all three
`db/kattis.db*` files together. Since the runbook already stops services before
backing up, a plain copy is safe there; just don't hot-copy only the main file.
(`*.db*` in `.gitignore` already excludes the sidecars from git.)

Unit files are **copied** into `/etc/systemd/system/`, not symlinked from the
repo (so `systemctl enable services/*.service` fails — they already exist
there). After any `.service` change, the deploy must `sudo cp services/*.service
/etc/systemd/system/` + `daemon-reload`, else systemd runs the stale unit. The
units' `ExecStart` points at `services/start*.sh`, so `chmod +x` those too.

## Deferred / future work
- **Per-affiliation/country aggregate backstop** — if an affiliation drops out
  of `/ranklist/affiliations` top 100 we lose its aggregate score series. Only
  per-*user* backstop exists today.
- **Slug-keyed bot reads** — migrate `history`/`get_top` and the `realname`
  table off display-name keying once the Kattis admin slug-dump lands. This
  fixes rename-breakage and duplicate-display-name conflation.
- **Bot UX** — `/track-user` exists; a fuller tracking UI (list/untrack) could
  follow. (The old `$kattis` parser and its latent bugs — multiple `=`,
  non-numeric `top=`/`days=`, `<@!`-only mentions, `IN ()` — are gone: slash
  typed params, choices, and the native `member:` picker replaced them.)
- **Problem graphing in the bot** — the problem scraper stores `problem_*`
  time-series, but the bot has no read path / command to plot them yet
  (difficulty/submissions/verdict trends, toplists). To add later.
  - ⚠️ **"Current toplist" must key off `problem_obs`, NOT `MAX(problem_toplist.timestamp)`.**
    A tie (score-tie or time-tie) makes Kattis leave the "All languages" list
    empty, so that scrape writes **zero** `problem_toplist` rows for that kind.
    Taking `MAX(timestamp)` over `problem_toplist` therefore resurrects the last
    *non-empty* (possibly long-stale) snapshot and shows a user as still holding
    e.g. the fastest solution after they've been tied out of it. Correct pattern:
    per problem take `latest = MAX(timestamp) FROM problem_obs` (every scrape
    writes `problem_obs`), then read `problem_toplist` rows `WHERE timestamp =
    latest` — absent rows mean genuinely empty/tied (no holder), not stale.
    This bug previously existed in the parser too (empty all-languages `_0` block
    bled the first per-language table in as rank 1 — see "Empty toplists"); fixed,
    and the contaminated history was deleted on 2026-06-05 (one latest snapshot
    per problem retained), but the read-side pitfall above is independent and is
    still latent for whoever adds the bot command.
  - ⚠️ **"Fastest untied" means rank-1 runtime STRICTLY lower than rank 2 — not
    just "appears at rank 1".** Kattis only blanks the all-languages list on a
    *big* runtime tie; a small tie (2–3 users at the same displayed runtime, e.g.
    several `0.00`s) is still listed, ordered by submission date. So a populated
    list does NOT imply an untied #1. To attribute a fastest title, require
    `value(rank1) < value(rank2)` (or rank 2 absent). Counting bare rank-1 rows
    overcounts: Vincent Lagerros was 706 by bare-rank-1 vs 566 truly-untied; the
    140-gap is all shared-runtime ties. (Same idea applies to `shortest` —
    byte-length ties — if you ever attribute "shortest" titles.)
  - ⚠️ **A problem that transitions from having a toplist to NOT having one must
    drop its attribution entirely — never carry the old holder forward.** If a
    problem stops showing a top list (it had rank-1..N rows at an earlier scrape,
    then a later scrape has none — newly tied/blanked, or the section vanished),
    the keep-latest-snapshot rule already handles this *as long as* you key off
    `problem_obs` (previous warning): the latest `problem_obs` timestamp has zero
    matching `problem_toplist` rows, so the user is correctly attributed nothing.
    Do NOT fall back to an earlier snapshot to "fill in" a now-empty list — an
    absent current toplist is a real state (no untied holder), not missing data.
