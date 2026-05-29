"""Pure rendering layer for the Kattis history graphs.

No Discord, no DB, no argument parsing — just `PlotRequest` + series -> PNG
bytes, so it is unit-testable offline. Uses the object-oriented Matplotlib
`Figure` API (NOT the global `pyplot` state), because the bot renders inside
`asyncio.to_thread` and global pyplot state is not thread-safe across
concurrent `/kattis` invocations.
"""

import enum
import io
from dataclasses import dataclass
from datetime import datetime

from matplotlib.figure import Figure
from matplotlib.ticker import MaxNLocator

# Above this many lines the legend clutters more than it helps, so it's hidden.
_LEGEND_MAX_LINES = 10


class Metric(enum.Enum):
    # value doubles as the y-axis label.
    SCORE = "Score"
    RANK = "Rank"
    NUM_USERS = "#users"
    NUM_AFFILIATIONS = "#unis"


class Scope(enum.Enum):
    GLOBAL = "global"
    SWE = "swe"
    CHALMERS = "chalmers"
    ALL = "all"


@dataclass(frozen=True)
class PlotRequest:
    metric: Metric
    scope: Scope
    days: int
    log: bool
    entity_kind: str           # 'user' | 'uni' | 'country' — for context only


Series = list[tuple[str, list[tuple[datetime, float]]]]


def render(req: PlotRequest, series: Series) -> bytes:
    """Render the given (label, [(date, value), ...]) series to PNG bytes.

    Series with no points are skipped.
    """
    fig = Figure()
    ax = fig.add_subplot(111)

    n_lines = 0
    for label, points in series:
        if not points:
            continue
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        ax.plot(xs, ys, label=label)
        n_lines += 1

    ax.set_ylabel(req.metric.value)
    if req.metric is not Metric.SCORE:
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))  # only integer ticks
    if req.metric is Metric.RANK:
        ax.invert_yaxis()  # rank 1 at the top
    if req.log:
        ax.set_yscale("log")
    ax.tick_params(axis="x", labelrotation=20)

    if 0 < n_lines <= _LEGEND_MAX_LINES:
        handles, labels = ax.get_legend_handles_labels()
        # Order the legend best-first: ascending rank, descending otherwise.
        sign = 1 if req.metric is Metric.RANK else -1
        labels, handles = zip(*sorted(
            zip(labels, handles),
            key=lambda t: sign * t[1].get_ydata()[-1],
        ))
        ax.legend(handles, labels)

    ax.grid(True)

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    return buf.getvalue()
