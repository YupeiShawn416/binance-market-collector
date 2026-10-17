"""Browser-free PNG rendering of the same reviewed quality grid."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path


def render_png(data: dict, output):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as dates
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.colors import BoundaryNorm, ListedColormap
    from matplotlib.patches import Patch

    from .preview_html import STATE_COLORS, STATE_LABELS

    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError(f"refusing to replace {output}")
    meta = data["metadata"]
    x = dates.date2num([datetime.fromtimestamp(t / 1e9, UTC) for t in data["time_edges_ns"]])
    centers = np.array(data["price_centers"])
    step = meta["price_bin_quote"]
    y = np.append(centers - step / 2, centers[-1] + step / 2)
    quantities = np.array(data["depth_base"], dtype=float)
    z = np.ma.masked_invalid(np.log1p(quantities))
    cmap = plt.get_cmap("cividis").with_extremes(bad="#e8ebee")
    figure, axes = plt.subplots(
        3, 1, figsize=(14, 9), sharex=True, gridspec_kw={"height_ratios": [6, 1, 2], "hspace": 0.16}
    )
    try:
        figure.subplots_adjust(left=0.085, right=0.87, top=0.88, bottom=0.16)
        figure.suptitle(
            f"{meta['instrument']} | Observed market-data quality",
            x=0.085,
            ha="left",
            y=0.97,
            fontsize=19,
        )
        figure.text(
            0.085,
            0.925,
            f"{meta['selected_events']:,} events   |   {meta['counts']['Trade']:,} trades   |   "
            f"{meta['missing_trade_ids']} missing trade IDs   |   "
            f"{meta['valid_book_fraction']:.1%} valid book time",
            fontsize=11,
        )
        figure.text(
            0.085,
            0.902,
            f"UTC {datetime.fromtimestamp(meta['start_ns'] / 1e9, UTC):%Y-%m-%d %H:%M:%S.%f}"[:-3]
            + f" to {datetime.fromtimestamp(meta['end_ns'] / 1e9, UTC):%H:%M:%S.%f}"[:-3],
            fontsize=9,
        )
        heat = axes[0].pcolormesh(
            x,
            y,
            z,
            cmap=cmap,
            vmin=0,
            vmax=max(float(z.max()) if z.count() else 0, 1e-6),
            rasterized=True,
        )
        cax = figure.add_axes([0.89, 0.51, 0.015, 0.32])
        colorbar = figure.colorbar(heat, cax=cax)
        maximum = float(np.nanmax(quantities)) if np.isfinite(quantities).any() else 0
        positions = np.linspace(0, max(float(np.log1p(maximum)), 1e-6), 5) if maximum else [0]
        colorbar.set_ticks(positions, labels=[f"{np.expm1(t):.3g}" for t in positions])
        colorbar.set_label(f"Visible depth ({meta['base']}); log color scale")
        midpoints = (x[:-1] + x[1:]) / 2
        for name, values, color in (
            ("Best bid", data["best_bid"], "#1d78b6"),
            ("Best ask", data["best_ask"], "#d65a2e"),
        ):
            axes[0].plot(
                midpoints,
                [np.nan if v is None else v for v in values],
                label=name,
                color=color,
                linewidth=1,
            )
        axes[0].set_ylabel(f"Price ({meta['quote']})")
        axes[0].legend(loc="upper left", fontsize=9)
        states = list(STATE_LABELS)
        values = [
            [states.index(s) for s in data["statuses"][scope]]
            for scope in ("connection", "book", "trades")
        ]
        axes[1].pcolormesh(
            x,
            np.arange(4),
            values,
            cmap=ListedColormap(STATE_COLORS),
            norm=BoundaryNorm(np.arange(7) - 0.5, 6),
        )
        axes[1].set_yticks([0.5, 1.5, 2.5], ["Connection", "Book", "Trades"])
        for event in data["quality_events"]:
            when = dates.date2num(datetime.fromtimestamp(event["time_ns"] / 1e9, UTC))
            scope = {"connection": 0.5, "book": 1.5, "trades": 2.5}[event["scope"]]
            axes[1].plot(when, scope, marker="x", color="#161616", markersize=5)
        axes[2].bar(
            midpoints, data["buy_base"], width=np.diff(x), label="Aggressive buy", color="#296b9b"
        )
        axes[2].bar(
            midpoints,
            data["sell_base"],
            bottom=data["buy_base"],
            width=np.diff(x),
            label="Aggressive sell",
            color="#bc6a32",
        )
        axes[2].set_ylabel(f"Traded {meta['base']}")
        axes[2].legend(loc="upper left", fontsize=9)
        axes[2].xaxis.set_major_formatter(dates.DateFormatter("%H:%M:%S", tz=UTC))
        axes[2].set_xlabel(
            f"Normalization time (UTC), {datetime.fromtimestamp(meta['start_ns'] / 1e9, UTC):%Y-%m-%d}"
        )
        for axis in axes:
            axis.set_facecolor("#e8ebee")
            axis.set_xlim(x[0], x[-1])
            axis.spines[["top", "right"]].set_visible(False)
        figure.legend(
            [Patch(color=color) for color in STATE_COLORS],
            states,
            ncols=6,
            loc="lower left",
            bbox_to_anchor=(0.08, 0.065),
            frameon=False,
            fontsize=9,
        )
        figure.text(
            0.085,
            0.035,
            f"Gray = unknown/invalid/partially covered, not zero. Cells: {meta['time_bin_ms']:g}ms x "
            f"{step:g} {meta['quote']}. Depth = time-weighted mean; trades = per-bin sum.",
            fontsize=9,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as stream:
            figure.savefig(stream, format="png", dpi=160, facecolor="white")
    finally:
        plt.close(figure)
    return str(output)
