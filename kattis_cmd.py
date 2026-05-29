"""The `/kattis` slash command group (user / uni / country) and its glue.

Thin Discord layer: it resolves the requested entities, calls the DB read path
(`KattisDbConn.history` / `get_top`), and hands a `PlotRequest` + series to the
pure `plot.render`. All the metric-vs-type validation is structural — each
subcommand only offers the metrics valid for its type — so there is no runtime
combination check.

`setup(kattis_conn, user_conn)` wires the module-level DB handles and returns
the group for `client.tree.add_command(...)`.
"""

import asyncio
import io
from datetime import datetime, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.app_commands import Choice

import plot
from plot import Metric, Scope, PlotRequest


# Set by setup() at startup.
kattis_conn = None
user_conn = None


# ---- choice value <-> plot enum maps ----------------------------------------
_METRIC = {
    "score": Metric.SCORE, "rank": Metric.RANK,
    "num_users": Metric.NUM_USERS, "num_affiliations": Metric.NUM_AFFILIATIONS,
}
_METRIC_ATTR = {
    Metric.SCORE: "score", Metric.RANK: "rank",
    Metric.NUM_USERS: "num_users", Metric.NUM_AFFILIATIONS: "num_affiliations",
}
_SCOPE = {"global": Scope.GLOBAL, "swe": Scope.SWE, "chalmers": Scope.CHALMERS}

SCORE_C = Choice(name="score", value="score")
RANK_C = Choice(name="rank", value="rank")
NUSERS_C = Choice(name="num_users", value="num_users")
NAFF_C = Choice(name="num_affiliations", value="num_affiliations")
# Scope choices are restricted per type to the ranklists that actually exist
# (see _allowed_contexts): users have global/swe/chalmers, affiliations only
# global/swe, countries only global (so /kattis country takes no scope at all).
SCOPE_CHOICES_USER = [Choice(name=s, value=s) for s in ("global", "swe", "chalmers")]
SCOPE_CHOICES_UNI = [Choice(name=s, value=s) for s in ("global", "swe")]

_BASE_DESC = {
    "names": "Comma-separated names; autocompletes against tracked names.",
    "days": "Only show the last N days (default: all history).",
    "metric": "What to plot (default: score).",
    "top": "Also include the top-N by score in the chosen scope (0 = none).",
    "log": "Use a logarithmic y-axis.",
}
_SCOPE_DESC = {"scope": "Which ranklist to read from (matters for rank; default depends on metric)."}
_USER_DESC = {**_BASE_DESC, **_SCOPE_DESC, "member": "A Discord member; uses their /setname."}
_UNI_DESC = {**_BASE_DESC, **_SCOPE_DESC}
_COUNTRY_DESC = _BASE_DESC

_DEFAULT_DAYS = 10 ** 5
_TOP_FALLBACK = 5  # used when nothing is specified


def _default_scope(kind, metric):
    # rank is a position within ONE ranklist and differs across contexts, so it
    # must never resolve to 'all' (which would merge/mix ranklists). Pick a
    # concrete default: chalmers for users (legacy, Chalmers-biased bot), global
    # otherwise. score/#users/#unis are context-invariant, so 'all' is fine.
    if metric is Metric.RANK:
        return Scope.CHALMERS if kind == "user" else Scope.GLOBAL
    return Scope.ALL


group = app_commands.Group(name="kattis", description="Plot Kattis ranklist history.")


async def _names_autocomplete(interaction: discord.Interaction, current: str):
    # Complete only the last comma-separated token, preserving what's typed.
    kind = interaction.command.name  # 'user' | 'uni' | 'country'
    head, sep, last = current.rpartition(",")
    matches = kattis_conn.distinct_display_names(kind, last.strip(), 25)
    out = []
    for m in matches:
        full = ((head + sep + " ") if sep else "") + m
        full = full[:100]  # Discord's Choice name/value cap
        out.append(Choice(name=full, value=full))
    return out


async def _run(interaction, kind, names, days, metric, scope, top, log, member=None):
    # metric/scope arrive as raw choice values (str) or None when omitted.
    pmetric = _METRIC[metric or "score"]

    requested, seen = [], set()
    def add(n):
        if n and n not in seen:
            seen.add(n)
            requested.append(n)

    for tok in names.split(","):
        add(tok.strip())
    if member is not None:
        rn = user_conn.get_realname(str(member.id))
        if rn is None:
            await interaction.response.send_message(
                f"{member.display_name} has no name set — they can use /setname.",
                ephemeral=True)
            return
        add(rn)

    # Nothing specified: plot the caller (user subcommand) else top-5 global.
    used_top = top
    if not requested and top == 0:
        if kind == "user":
            rn = user_conn.get_realname(str(interaction.user.id))
            if rn:
                add(rn)
            else:
                used_top = _TOP_FALLBACK
        else:
            used_top = _TOP_FALLBACK

    pscope = _SCOPE[scope] if scope else _default_scope(kind, pmetric)

    if used_top > 0:
        # Pull the top from the chosen scope so the names actually have rows in
        # it; 'all' isn't a single ranklist, so fall back to the global list.
        top_ctx = "global" if pscope is Scope.ALL else pscope.value
        for n in (kattis_conn.get_top(kind, top_ctx, used_top) or []):
            add(n)

    if not requested:
        await interaction.response.send_message(
            "Nothing to plot — pass `names`, a `member`, or `top`.", ephemeral=True)
        return

    mint = int((datetime.now() - timedelta(days=days)).timestamp())
    hist = kattis_conn.history(mint, kind, requested, pscope.value)

    attr = _METRIC_ATTR[pmetric]
    series, missing = [], []
    for name, rows in hist:
        # Drop points where the metric is NULL (e.g. historical rows with no
        # num_users) so None never reaches matplotlib.
        pts = [(datetime.fromtimestamp(r.timestamp), getattr(r, attr))
               for r in rows if getattr(r, attr) is not None]
        if pts:
            series.append((name, pts))
        else:
            missing.append(name)

    if not series:
        await interaction.response.send_message(
            "Nothing to show — no data for: " + ", ".join(requested), ephemeral=True)
        return

    # Public graph. DB reads above are fast (local sqlite); defer here so the
    # CPU-bound render has the full 15-min followup window.
    await interaction.response.defer(thinking=True)
    req = PlotRequest(metric=pmetric, scope=pscope, days=days, log=log, entity_kind=kind)
    try:
        png = await asyncio.to_thread(plot.render, req, series)
    except Exception as e:
        # Already deferred (public "thinking…"); we MUST follow up or it hangs.
        await interaction.followup.send(f"Failed to render the graph: {e}")
        return
    content = ("couldn't find: " + ", ".join(missing)) if missing else None
    await interaction.followup.send(
        content=content, file=discord.File(io.BytesIO(png), filename="graph.png"))


@group.command(name="user", description="Plot Kattis users' score/rank history.")
@app_commands.describe(**_USER_DESC)
@app_commands.choices(metric=[SCORE_C, RANK_C], scope=SCOPE_CHOICES_USER)
@app_commands.autocomplete(names=_names_autocomplete)
async def kattis_user(interaction: discord.Interaction,
                      names: str = "",
                      days: app_commands.Range[int, 1, None] = _DEFAULT_DAYS,
                      metric: Optional[str] = None,
                      scope: Optional[str] = None,
                      top: app_commands.Range[int, 0, 50] = 0,
                      member: Optional[discord.Member] = None,
                      log: bool = False):
    await _run(interaction, "user", names, days, metric, scope, top, log, member=member)


@group.command(name="uni", description="Plot affiliations' score/rank/#users history.")
@app_commands.describe(**_UNI_DESC)
@app_commands.choices(metric=[SCORE_C, RANK_C, NUSERS_C], scope=SCOPE_CHOICES_UNI)
@app_commands.autocomplete(names=_names_autocomplete)
async def kattis_uni(interaction: discord.Interaction,
                     names: str = "",
                     days: app_commands.Range[int, 1, None] = _DEFAULT_DAYS,
                     metric: Optional[str] = None,
                     scope: Optional[str] = None,
                     top: app_commands.Range[int, 0, 50] = 0,
                     log: bool = False):
    await _run(interaction, "uni", names, days, metric, scope, top, log)


@group.command(name="country", description="Plot countries' score/rank/#users/#unis history.")
@app_commands.describe(**_COUNTRY_DESC)
@app_commands.choices(metric=[SCORE_C, RANK_C, NUSERS_C, NAFF_C])
@app_commands.autocomplete(names=_names_autocomplete)
async def kattis_country(interaction: discord.Interaction,
                         names: str = "",
                         days: app_commands.Range[int, 1, None] = _DEFAULT_DAYS,
                         metric: Optional[str] = None,
                         top: app_commands.Range[int, 0, 50] = 0,
                         log: bool = False):
    # country has only a global ranklist, so there is no scope option.
    await _run(interaction, "country", names, days, metric, None, top, log)


def setup(k, u):
    """Wire the DB handles; returns the group for client.tree.add_command()."""
    global kattis_conn, user_conn
    kattis_conn, user_conn = k, u
    return group
