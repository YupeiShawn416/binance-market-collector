"""Optional Plotly presentation; the aggregation/reducer has no plotting dependency."""

from __future__ import annotations

import html
import math
from datetime import UTC, datetime
from pathlib import Path

from .preview import PreviewOptions, build_preview

STATE_LABELS = {
    "LIVE": "有效",
    "SYNCING": "同步中",
    "PARTIAL": "部分未知",
    "STALE": "过期",
    "INVALID": "无效",
    "DISCONNECTED": "断线",
}
STATE_COLORS = ["#2d735d", "#aa863b", "#a58b60", "#8259a2", "#b54b40", "#52606e"]


def _iso(ns):
    return datetime.fromtimestamp(ns / 1e9, UTC).isoformat(timespec="milliseconds")


def preview_html(db, output, options: PreviewOptions | None = None, *, png=None):
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError as exc:
        raise RuntimeError("preview requires: uv sync --frozen --extra preview") from exc
    output = Path(output).resolve()
    source = Path(db).resolve()
    if output == source or output in {
        Path(str(source) + suffix) for suffix in ("-wal", "-shm", ".lockfile")
    }:
        raise ValueError("preview output overlaps source journal")
    if output.exists():
        raise FileExistsError(f"refusing to replace {output}; use a new output path")
    if png is not None:
        png_path = Path(png).resolve()
        if (
            png_path == output
            or png_path == source
            or png_path in {Path(str(source) + suffix) for suffix in ("-wal", "-shm", ".lockfile")}
        ):
            raise ValueError("PNG output overlaps another output or source journal")
        if png_path.exists():
            raise FileExistsError(f"refusing to replace {png_path}")
    data = build_preview(db, options)
    meta, edges = data["metadata"], data["time_edges_ns"]
    x = [_iso((a + b) // 2) for a, b in zip(edges, edges[1:], strict=False)]
    figure = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.045, row_heights=[0.65, 0.13, 0.22]
    )
    actual = data["depth_base"]
    z = [[math.log1p(v) if v is not None else None for v in row] for row in actual]
    max_quantity = max((v for row in actual for v in row if v is not None), default=0)
    scale_max = max(math.log1p(max_quantity), 1e-6)
    ticks = [math.expm1(scale_max * i / 4) for i in range(5)] if max_quantity else [0]
    figure.add_trace(
        go.Heatmap(
            x=x,
            y=data["price_centers"],
            z=z,
            customdata=actual,
            colorscale="Cividis",
            zmin=0,
            zmax=max(math.log1p(max_quantity), 1e-6),
            hoverongaps=False,
            colorbar={
                "title": {"text": f"深度 {meta['base']}"},
                "len": 0.56,
                "y": 0.72,
                "tickvals": [math.log1p(v) for v in ticks],
                "ticktext": [f"{v:.3g}" for v in ticks],
            },
            hovertemplate="%{x}<br>价格 %{y:,.2f}<br>时间加权深度 %{customdata:.8f} "
            + meta["base"]
            + "<extra></extra>",
            name="已观测挂单量",
        ),
        row=1,
        col=1,
    )
    for name, values, color in (
        ("最优买价", data["best_bid"], "#1d78b6"),
        ("最优卖价", data["best_ask"], "#d65a2e"),
    ):
        figure.add_trace(
            go.Scatter(
                x=x,
                y=values,
                mode="lines",
                name=name,
                line={"color": color, "width": 1.3},
                connectgaps=False,
            ),
            row=1,
            col=1,
        )
    states = list(STATE_LABELS)
    scales = []
    for i, color in enumerate(STATE_COLORS):
        scales.extend([(i / len(states), color), ((i + 1) / len(states), color)])
    scope_names = {"connection": "连接", "book": "盘口", "trades": "成交"}
    state_text = [
        [STATE_LABELS[state] for state in data["statuses"][scope]] for scope in scope_names
    ]
    figure.add_trace(
        go.Heatmap(
            x=[_iso(e) for e in edges],
            y=list(scope_names.values()),
            z=[[states.index(s) + 0.5 for s in data["statuses"][scope]] for scope in scope_names],
            customdata=state_text,
            zmin=0,
            zmax=len(states),
            colorscale=scales,
            showscale=False,
            hovertemplate="%{x}<br>%{y}：%{customdata}<extra></extra>",
            name="质量状态",
        ),
        row=2,
        col=1,
    )
    for name, values, color in (
        ("主动买成交", data["buy_base"], "#296b9b"),
        ("主动卖成交", data["sell_base"], "#bc6a32"),
    ):
        figure.add_trace(
            go.Bar(
                x=x,
                y=values,
                name=name,
                marker_color=color,
                width=[(b - a) / 1e6 for a, b in zip(edges, edges[1:], strict=False)],
                hovertemplate="%{x}<br>%{y:.8f} "
                + meta["base"]
                + "<extra>%{fullData.name}</extra>",
            ),
            row=3,
            col=1,
        )
    # Preserve instantaneous INVALID events even if LIVE follows in the same batch.
    markers = data["quality_events"]
    figure.add_trace(
        go.Scatter(
            x=[_iso(q["time_ns"]) for q in markers],
            y=[scope_names[q["scope"]] for q in markers],
            customdata=[
                html.escape(f"seq {q['seq']} · {q['state']} · {q['reason']}") for q in markers
            ],
            mode="markers",
            marker={"symbol": "x", "size": 8, "color": "#252525"},
            name="质量边界事件",
            hovertemplate="%{x}<br>%{customdata}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    figure.update_layout(
        template="plotly_white",
        height=820,
        margin={"l": 85, "r": 100, "t": 50, "b": 60},
        paper_bgcolor="#ffffff",
        plot_bgcolor="#e8ebee",
        font={"family": "Arial, sans-serif", "size": 12, "color": "#252f3a"},
        legend={"orientation": "h", "y": 1.07, "x": 0},
        barmode="stack",
        hovermode="closest",
    )
    figure.update_xaxes(type="date", range=[_iso(edges[0]), _iso(edges[-1])], showgrid=False)
    figure.update_xaxes(title_text="规范化时间（UTC；事件顺序不变）", row=3, col=1)
    figure.update_yaxes(title_text=f"价格 ({meta['quote']})", row=1, col=1)
    figure.update_yaxes(title_text=f"成交 ({meta['base']})", row=3, col=1)
    chart = figure.to_html(
        full_html=False,
        include_plotlyjs=True,
        div_id="quality-chart",
        config={
            "responsive": True,
            "displaylogo": False,
            "scrollZoom": False,
            "toImageButtonOptions": {"filename": "market-quality", "scale": 2},
        },
    )
    body_rows = "".join(
        f"<tr><td>{q['seq']}</td><td>{html.escape(_iso(q['time_ns']))}</td>"
        f"<td>{scope_names[q['scope']]}</td><td>{html.escape(q['state'])}</td>"
        f"<td>{html.escape(q['reason'])}</td></tr>"
        for q in markers
    )
    status_legend = " ".join(
        f'<span style="border-left:10px solid {color};padding-left:6px">{label}</span>'
        for label, color in zip(STATE_LABELS.values(), STATE_COLORS, strict=True)
    )
    cards = [
        ("规范事件", f"{meta['selected_events']:,}"),
        ("观察成交", f"{meta['counts']['Trade']:,}"),
        ("缺失成交 ID", str(meta["missing_trade_ids"])),
        ("盘口有效时长", f"{meta['valid_book_fraction']:.1%}"),
        ("原始待处理", str(meta["pending_raw"])),
    ]
    card_html = "".join(
        f"<div><small>{label}</small><strong>{value}</strong></div>" for label, value in cards
    )
    page = f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>行情数据质量预览</title>
<style>body{{margin:0;background:#f3f5f7;color:#252f3a;font:15px/1.65 Arial,sans-serif}}main{{max-width:1400px;margin:auto;padding:28px}}h1{{font-size:26px;margin:0}}p{{margin:10px 0}}.cards{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin:22px 0}}.cards div{{background:white;padding:14px 18px;border:1px solid #dae0e5}}small{{display:block;color:#536273}}strong{{display:block;font-size:25px}}.chart{{background:white;padding:12px}}.legend{{display:flex;flex-wrap:wrap;gap:18px;margin:16px 0;font-size:13px}}.table{{overflow:auto;background:white}}table{{border-collapse:collapse;width:100%;font-size:13px}}th,td{{padding:10px;border-bottom:1px solid #e4e8eb;text-align:left;white-space:nowrap}}code{{overflow-wrap:anywhere}}details{{margin:18px 0}}@media(max-width:650px){{main{{padding:14px}}.cards{{grid-template-columns:repeat(2,1fr)}}h1{{font-size:22px}}}}</style></head><body><main>
<h1>{html.escape(meta["instrument"])} · 采集数据质量预览</h1>
<p>{html.escape(_iso(meta["start_ns"]))} → {html.escape(_iso(meta["end_ns"]))} · 原始观察口径 · seq {meta["first_seq"]}–{meta["last_seq"]}</p>
<div class="cards">{card_html}</div>
<p><b>先看空白：</b>灰色是无效、过期、部分缺失或快照范围外的未知单元；有效覆盖内的零挂单才显示为色阶最低值。断线区间不延续旧盘口。</p>
<div class="chart">{chart}</div><div class="legend">{status_legend}</div>
<p>每格是 {meta["time_bin_ms"]:g}ms × {meta["price_bin_quote"]:g} {html.escape(meta["quote"])} 内的时间加权可见挂单量；色阶为 log(1+数量)，刻度和悬停仍显示 {html.escape(meta["base"])}。仅完整有效覆盖的格子着色；两侧数量按价位合计，不能据此重建订单队列。</p>
<p>质量条按格内最差状态显示，× 标记保留瞬时质量边界。成交量按格求和，均为非负堆叠。最优报价为每格末次已知值；只在完整有效盘口列显示。</p>
<p>图表支持拖拽放大、双击复位、图例开关与工具栏 PNG 导出。HTML 包含本地 Plotly，无 CDN 请求；它是生成时固定视图，不自动刷新。</p>
<details><summary>数据与校验口径</summary><p>固定 WAL 读取事务；从检查点 {meta["checkpoint_seq"]} 开始校验之后的事件链和盘口哈希，并核验所选事件引用的 {meta["raw_inputs_checked"]} 个原始输入。不是整库所有历史的完整校验，整库核查请用 verify。</p>
<p>dataset: <code>{html.escape(meta["dataset_id"])}</code><br>末事件链: <code>{meta["final_event_chain"]}</code><br>末盘口哈希: <code>{meta["final_book_hash"]}</code></p>
<p>盘口过期阈值 {meta["stale_seconds"]:g} 秒。规范化墙钟回退 {meta["wall_clock_regressions_clamped"]} 次，展示时按事件顺序单调化，不改原日志。窗口最多显示配置数量的尾部事件；未显示范围不代表没有缺口。</p>
<p>缺失成交 ID 计数来自窗口中的显式 trade_id_gap；不包含窗口前已存在的缺口，不查询事后回补目录，也不宣称交易所全历史完整。</p></details>
<h2>质量边界</h2><p>最多列出200条非 LIVE 边界；精确原因同时可在质量条标记悬停查看。</p>
<div class="table"><table><thead><tr><th>序列</th><th>时间 UTC</th><th>域</th><th>状态</th><th>原因</th></tr></thead><tbody>{body_rows}</tbody></table></div>
</main></body></html>"""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        stream.write(page)
    result = {**meta, "output": str(output)}
    if png is not None:
        from .preview_png import render_png

        result["png"] = render_png(data, png)
    return result
