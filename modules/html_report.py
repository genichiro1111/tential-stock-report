"""
Module 5: HTML Report Generator
モックと同等のリッチHTMLレポートを自動生成する
SVGチャート・ヒートマップ・ブランドカラー完全対応
週タブナビゲーション付き
"""
import datetime, logging, math, html as html_mod, glob, re
from pathlib import Path
from typing import Dict, List, Optional
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import BRAND_COLORS, CATEGORY_ORDER
from modules.quantitative import QuantReport, StockPerformance, CategoryPerformance, DailyBar
from modules.qualitative import QualReport, NewsItem, ForumPost, TopicCategory

logger = logging.getLogger(__name__)

# ─── Brand Colors ────────────────────────────────────────────────
C = BRAND_COLORS
NAVY = C["navy"]; BLUE = C["blue"]; BEIGE = C["beige"]
GRAY = C["gray"]; DARK = C["dark"]; LIGHT = C["light"]
RED = C["red"]; GREEN = C["green"]

def _esc(s):
    return html_mod.escape(str(s))

def _fp(v):
    """Format percent with sign"""
    s = "+" if v > 0 else ""
    return f"{s}{v:.1f}%"

def _pcolor(v):
    """Return color based on positive/negative value (JP market convention)"""
    if v > 0.5: return RED     # positive = red (up)
    if v < -0.5: return GREEN  # negative = green (down)
    return GRAY

def _minkabu_url(code: str) -> str:
    """みんかぶの銘柄ページURL（国内・海外両対応）"""
    # 海外銘柄: アルファベットのみ（数字を含まない）→ US stock
    if code.isalpha():
        return f"https://us.minkabu.jp/stocks/{code}"
    return f"https://minkabu.jp/stock/{code}"

def _stock_link(name: str, code: str, star: str = "") -> str:
    """銘柄名をみんかぶへのリンクにする"""
    url = _minkabu_url(code)
    return f'<a href="{url}" target="_blank" style="color:inherit;text-decoration:none;border-bottom:1px dotted {GRAY}" title="みんかぶで見る">{_esc(name)}{star}</a> <span style="color:{GRAY};font-size:11px">{_esc(code)}</span>'

def _heat_bg(v, max_abs=5.0):
    """Return a CSS background color for heatmap cell"""
    clamped = max(-max_abs, min(max_abs, v))
    intensity = abs(clamped) / max_abs
    if clamped > 0:
        r, g, b = 201, 68, 68  # RED
        alpha = intensity * 0.35
    elif clamped < 0:
        r, g, b = 46, 139, 87  # GREEN
        alpha = intensity * 0.35
    else:
        return "transparent"
    return f"rgba({r},{g},{b},{alpha:.2f})"

def _spark_svg(values, width=120, height=32, color=NAVY):
    """Generate inline SVG sparkline"""
    if not values or len(values) < 2:
        return ""
    mn, mx = min(values), max(values)
    rng = mx - mn if mx != mn else 1
    pts = []
    for i, v in enumerate(values):
        x = (i / (len(values) - 1)) * width
        y = height - ((v - mn) / rng) * (height - 4) - 2
        pts.append(f"{x:.1f},{y:.1f}")
    polyline = " ".join(pts)
    return f'<svg width="{width}" height="{height}" style="vertical-align:middle"><polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linecap="round"/></svg>'


class HTMLReportGenerator:
    def __init__(self, quant: QuantReport, qual: QualReport, metadata: dict):
        self.q = quant
        self.ql = qual
        self.meta = metadata

    def generate(self) -> str:
        """Generate complete HTML report as a string"""
        t = self.q.tential
        ws = self.meta.get("week_start", "")
        we = self.meta.get("week_end", "")
        wn = datetime.date.today().isocalendar()[1]
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

        sections = [
            self._section_executive_summary(),
            self._section_market_overview(),
            self._section_tential_performance(),
            self._section_comps_heatmap(),
            self._section_comps_ranking(),
            self._section_category_summary(),
            self._section_margin_analysis(),
            self._section_qualitative(),
            self._section_watchpoints(),
        ]

        body = "\n".join(sections)

        return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TENTIAL Weekly Stock Report — W{wn} ({ws} 〜 {we})</title>
{self._css()}
</head>
<body>
<div class="container">
  <header class="report-header">
    <div class="header-top">
      <div class="brand">
        <svg class="brand-logo" width="48" height="48" viewBox="760 280 410 510" xmlns="http://www.w3.org/2000/svg">
          <rect x="760" y="290" fill="#E4C9A5" width="290.9" height="110"/>
          <path fill="#567EB0" d="M1044.1,290c-16.5,0-24.1,9.3-24.1,24.7V400h140V290H1044.1z"/>
          <path fill="#284B7D" d="M1036.6,290c-6.2,0-11.3,2.7-16.1,6.3L903.8,396.7c-2.4,2.1-3.8,5.1-3.8,8.3v385l120,0V314.7c0.3-14.6,9.6-21.2,14-23.1c3.4-1.4,8.1-1.6,10.2-1.7C1042.5,290,1039.2,290,1036.6,290z"/>
        </svg>
        <div>
          <h1>Weekly Stock Report</h1>
          <p class="subtitle">W{wn} — {ws} 〜 {we}</p>
        </div>
      </div>
      <div class="header-meta">
        <span class="badge">325A / グロース</span>
        <span class="timestamp">Generated: {now}</span>
      </div>
    </div>
  </header>
  {self._week_tabs(ws)}
  <main>
    {body}
  </main>
  <footer>
    <p>📊 TENTIAL IR — Weekly Stock Performance Report（自動生成）</p>
  </footer>
</div>
</body>
</html>"""

    # ──────────────────────────────────────────────
    # Week Tabs
    # ──────────────────────────────────────────────
    def _week_tabs(self, current_week_start: str) -> str:
        """Generate week navigation tabs by scanning output directory for existing reports"""
        output_dir = Path(__file__).resolve().parent.parent / "output"
        files = sorted(output_dir.glob("weekly_report_*.html"), reverse=True)
        weeks = []
        current_file = f"weekly_report_{current_week_start.replace('-', '')}.html"
        for f in files:
            m = re.search(r"weekly_report_(\d{8})\.html", f.name)
            if m:
                ds = m.group(1)
                label = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
                # Calculate week number
                try:
                    d = datetime.date(int(ds[:4]), int(ds[4:6]), int(ds[6:]))
                    wn = d.isocalendar()[1]
                    label = f"W{wn}"
                except:
                    pass
                is_active = f.name == current_file
                weeks.append((label, f.name, is_active))
        # Also add current if not yet in list
        if not any(w[2] for w in weeks):
            ds = current_week_start.replace("-", "")
            try:
                d = datetime.date(int(ds[:4]), int(ds[4:6]), int(ds[6:]))
                wn = d.isocalendar()[1]
                weeks.insert(0, (f"W{wn}", current_file, True))
            except:
                weeks.insert(0, (current_week_start, current_file, True))
        if len(weeks) <= 1:
            return ""
        tabs = ""
        for label, fname, active in weeks[:12]:  # Show last 12 weeks max
            cls = "week-tab active" if active else "week-tab"
            tabs += f'<a href="{fname}" class="{cls}">{label}</a>'
        return f'<nav class="week-nav">{tabs}</nav>'

    # ──────────────────────────────────────────────
    # CSS
    # ──────────────────────────────────────────────
    def _css(self):
        return f"""<style>
:root {{
  --navy: {NAVY}; --blue: {BLUE}; --beige: {BEIGE};
  --gray: {GRAY}; --dark: {DARK}; --light: {LIGHT};
  --red: {RED}; --green: {GREEN};
  --radius: 12px;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Hiragino Kaku Gothic ProN", "Noto Sans JP", sans-serif;
  background: #f5f3ef; color: var(--dark); line-height: 1.6; }}
.container {{ max-width: 960px; margin: 0 auto; padding: 24px 16px; }}

/* Header */
.report-header {{ background: linear-gradient(315deg, var(--navy), var(--blue));
  color: #fff; padding: 32px; border-radius: var(--radius); margin-bottom: 24px; }}
.header-top {{ display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px; }}
.brand {{ display: flex; align-items: center; gap: 16px; }}
.brand-logo {{ width: 48px; height: 48px; flex-shrink: 0; }}
h1 {{ font-size: 22px; font-weight: 700; margin: 0; }}
.subtitle {{ font-size: 14px; opacity: 0.8; margin-top: 2px; }}
.header-meta {{ display: flex; align-items: center; gap: 12px; }}
.badge {{ background: rgba(255,255,255,0.2); padding: 4px 12px; border-radius: 20px; font-size: 13px; }}
.timestamp {{ font-size: 12px; opacity: 0.7; }}

/* Week Nav Tabs */
.week-nav {{ display: flex; gap: 6px; margin-bottom: 16px; padding: 4px;
  background: #fff; border-radius: var(--radius); overflow-x: auto;
  box-shadow: 0 1px 3px rgba(0,0,0,0.06); }}
.week-tab {{ padding: 8px 16px; border-radius: 8px; font-size: 13px; font-weight: 600;
  color: var(--gray); text-decoration: none; white-space: nowrap; transition: all 0.15s; }}
.week-tab:hover {{ background: #f0f0f0; color: var(--dark); }}
.week-tab.active {{ background: var(--navy); color: #fff; }}

/* Sections */
.section {{ background: #fff; border-radius: var(--radius); padding: 24px;
  margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.06); }}
.section-title {{ font-size: 16px; font-weight: 700; color: var(--navy);
  margin-bottom: 16px; padding-bottom: 8px; border-bottom: 2px solid var(--beige); display: flex; align-items: center; gap: 8px; }}

/* Callout */
.callout {{ background: #eef3f9; border-left: 4px solid var(--blue);
  padding: 16px 20px; border-radius: 0 8px 8px 0; margin-bottom: 16px;
  font-size: 15px; line-height: 1.7; }}
.callout.positive {{ background: #fef0f0; border-left-color: var(--red); }}
.callout.negative {{ background: #edf7f0; border-left-color: var(--green); }}
.callout.neutral {{ background: #f5f5f5; border-left-color: var(--gray); }}

/* KPI row */
.kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 16px; }}
.kpi {{ background: #f8f7f5; border-radius: 8px; padding: 16px; text-align: center; }}
.kpi-label {{ font-size: 12px; color: var(--gray); text-transform: uppercase; letter-spacing: 0.5px; }}
.kpi-value {{ font-size: 24px; font-weight: 700; color: var(--navy); margin: 4px 0; }}
.kpi-sub {{ font-size: 13px; }}

/* Tables */
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ background: var(--navy); color: #fff; padding: 10px 12px; text-align: left; font-weight: 600;
  position: sticky; top: 0; }}
th:first-child {{ border-radius: 8px 0 0 0; }}
th:last-child {{ border-radius: 0 8px 0 0; }}
td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
tr:hover td {{ background: #fafaf8; }}
.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.positive {{ color: var(--red); }}
.negative {{ color: var(--green); }}
.neutral {{ color: var(--gray); }}
.star {{ background: rgba(40,75,125,0.06); font-weight: 600; }}

/* Heatmap */
.heatmap td {{ text-align: center; font-weight: 600; font-size: 13px; transition: transform 0.1s; }}
.heatmap td:hover {{ transform: scale(1.05); }}
.heatmap td.cat-label {{ text-align: left; font-weight: 700; color: var(--navy); background: transparent !important; }}

/* Rankings */
.rank-item {{ display: flex; align-items: center; gap: 12px; padding: 8px 12px;
  border-radius: 8px; margin-bottom: 4px; }}
.rank-item:hover {{ background: #f8f7f5; }}
.rank-num {{ width: 28px; height: 28px; border-radius: 50%; display: flex;
  align-items: center; justify-content: center; font-size: 12px; font-weight: 700;
  color: #fff; flex-shrink: 0; }}
.rank-1 {{ background: #d4a017; }}
.rank-2 {{ background: #a0a0a0; }}
.rank-3 {{ background: #b87333; }}
.rank-other {{ background: #ddd; color: var(--dark); }}
.rank-name {{ flex: 1; font-size: 14px; }}
.rank-name .code {{ color: var(--gray); font-size: 12px; }}
.rank-value {{ font-size: 16px; font-weight: 700; font-variant-numeric: tabular-nums; }}
.rank-bar {{ width: 80px; height: 6px; background: #eee; border-radius: 3px; overflow: hidden; }}
.rank-bar-fill {{ height: 100%; border-radius: 3px; }}

/* Margin table */
.margin-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
.margin-card {{ background: #f8f7f5; border-radius: 8px; padding: 16px; }}
.margin-card h4 {{ font-size: 13px; color: var(--gray); margin-bottom: 8px; }}
.margin-card .val {{ font-size: 20px; font-weight: 700; color: var(--navy); }}
.margin-card .chg {{ font-size: 13px; margin-top: 4px; }}

/* Qualitative cards */
.qual-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
.qual-card {{ background: #f8f7f5; border-radius: 8px; padding: 16px; }}
.qual-card h4 {{ font-size: 14px; color: var(--navy); margin-bottom: 8px; }}
.qual-card p {{ font-size: 13px; line-height: 1.6; }}

/* Watch list */
.watch-item {{ display: flex; align-items: center; gap: 10px; padding: 10px 0;
  border-bottom: 1px solid #f0f0f0; font-size: 14px; }}
.watch-icon {{ font-size: 18px; }}

/* Footer */
footer {{ text-align: center; padding: 24px; font-size: 12px; color: var(--gray); }}

@media (max-width: 640px) {{
  .kpi-row {{ grid-template-columns: 1fr 1fr; }}
  .qual-grid {{ grid-template-columns: 1fr; }}
  .margin-grid {{ grid-template-columns: 1fr; }}
  h1 {{ font-size: 18px; }}
}}
</style>"""

    # ──────────────────────────────────────────────
    # Sections
    # ──────────────────────────────────────────────
    def _section_executive_summary(self):
        t = self.q.tential
        g = self.q.benchmarks.get("グロース250")
        vs_txt = ""
        callout_class = "neutral"
        if g:
            d = t.weekly_return - g.weekly_return
            direction = "アウトパフォーム" if d > 0 else "アンダーパフォーム"
            vs_txt = f"グロース250（{_fp(g.weekly_return)}）対比 {_fp(d)} で{direction}。"
            callout_class = "positive" if d > 0 else "negative"
        if t.weekly_return > 0:
            callout_class = "positive"
        elif t.weekly_return < 0:
            callout_class = "negative"

        # 需給主体分析 — margin data から売り手/買い手を推定
        m = self.q.margin
        supply_demand_html = self._supply_demand_analysis(m)

        return f"""<div class="section">
  <div class="section-title">🎯 Executive Summary</div>
  <div class="callout {callout_class}">
    TENTIAL（325A）は前週比 <strong>{_fp(t.weekly_return)}</strong>（終値 ¥{t.close:,.0f}）。{vs_txt}
  </div>
  {supply_demand_html}
</div>"""

    def _supply_demand_analysis(self, m):
        """需給の主体分析セクション生成"""
        if not m.long_balance and not m.short_balance:
            return ""

        # 信用買い残の状態判定
        long_trend = "整理中" if m.long_change_pct < 0 else ("積み増し" if m.long_change_pct > 2 else "横ばい")
        short_trend = "減少中（買い戻し）" if m.short_change_pct < -5 else ("増加中" if m.short_change_pct > 5 else "横ばい")

        # 売り手サイド
        sellers_html = f"""<div style="flex:1;min-width:280px">
  <h4 style="margin:0 0 10px;font-size:14px;font-weight:700;color:{RED}">📉 売り手（推定）</h4>
  <table style="width:100%;font-size:12px;border-collapse:collapse">
    <tr style="border-bottom:1px solid #f0f0f0">
      <td style="padding:6px 8px;font-weight:600">信用売り残</td>
      <td style="padding:6px 8px;text-align:right">{m.short_balance:,}株</td>
      <td style="padding:6px 8px;text-align:right;color:{_pcolor(-m.short_change_pct)}">{_fp(m.short_change_pct)}</td>
    </tr>
    <tr style="border-bottom:1px solid #f0f0f0">
      <td style="padding:6px 8px;font-weight:600">信用買い残整理</td>
      <td colspan="2" style="padding:6px 8px;text-align:right">{m.long_balance:,}株 — {long_trend}</td>
    </tr>
  </table>
  <div style="font-size:11px;color:{GRAY};margin-top:8px;line-height:1.6">
    空売り機関の動向は<a href="https://www.jpx.co.jp/markets/statistics-equities/short-selling/" target="_blank" style="color:{BLUE};text-decoration:none">JPX空売り残高</a>で確認可能。
    含み損からの追証・損切り売りが信用買い残の整理を加速。
  </div>
</div>"""

        # 買い手サイド
        ratio_comment = ""
        if m.ratio > 3:
            ratio_comment = "貸借倍率が高く（{:.1f}x）、買い残の重さが上値を抑制。".format(m.ratio)
        elif m.ratio < 1.5:
            ratio_comment = "貸借倍率が低下（{:.1f}x）、売り残の買い戻しが一巡。".format(m.ratio)
        else:
            ratio_comment = "貸借倍率 {:.1f}x。需給は中立的。".format(m.ratio)

        buyers_html = f"""<div style="flex:1;min-width:280px">
  <h4 style="margin:0 0 10px;font-size:14px;font-weight:700;color:{GREEN}">📈 買い手（推定）</h4>
  <table style="width:100%;font-size:12px;border-collapse:collapse">
    <tr style="border-bottom:1px solid #f0f0f0">
      <td style="padding:6px 8px;font-weight:600">個人投資家</td>
      <td style="padding:6px 8px;text-align:right">押し目買い（グロース小型では下支え）</td>
    </tr>
    <tr style="border-bottom:1px solid #f0f0f0">
      <td style="padding:6px 8px;font-weight:600">信用売り買い戻し</td>
      <td style="padding:6px 8px;text-align:right">{short_trend}</td>
    </tr>
  </table>
  <div style="font-size:11px;color:{GRAY};margin-top:8px;line-height:1.6">
    {_esc(ratio_comment)}
    詳細は<a href="https://kabutan.jp/stock/kabuka?code=325A&ashi=shin" target="_blank" style="color:{BLUE};text-decoration:none">株探 信用残</a>を参照。
  </div>
</div>"""

        return f"""
<div style="margin-top:16px">
  <h3 style="font-size:15px;font-weight:700;margin:0 0 12px;color:{NAVY}">⚖️ 需給の主体分析</h3>
  <div style="display:flex;gap:20px;flex-wrap:wrap">
    {sellers_html}
    {buyers_html}
  </div>
  <div style="margin-top:12px;padding:10px 14px;background:#f8f7f5;border-radius:8px;font-size:12px;color:{DARK};line-height:1.6">
    <strong>需給サマリー:</strong> 信用買い残 {m.long_balance:,}株（{_fp(m.long_change_pct)}）/ 信用売り残 {m.short_balance:,}株（{_fp(m.short_change_pct)}）/ 貸借倍率 {m.ratio:.2f}x
  </div>
</div>"""

    def _section_market_overview(self):
        rows = ""
        for name in ["日経平均", "TOPIX", "グロース250", "S&P 500"]:
            bm = self.q.benchmarks.get(name)
            if not bm:
                continue
            close_fmt = f"{bm.close:,.0f}" if bm.close > 100 else f"{bm.close:,.2f}"
            wc = _pcolor(bm.weekly_return)
            mc = _pcolor(bm.mtd_return)
            yc = _pcolor(bm.ytd_return)
            rows += f"""<tr>
  <td><strong>{_esc(name)}</strong></td>
  <td class="num">{close_fmt}</td>
  <td class="num" style="color:{wc}">{_fp(bm.weekly_return)}</td>
  <td class="num" style="color:{mc}">{_fp(bm.mtd_return)}</td>
  <td class="num" style="color:{yc}">{_fp(bm.ytd_return)}</td>
</tr>"""

        ms = self.ql.market_sentiment
        memo = ""
        if ms.jp_market_summary:
            memo = f'<div class="callout" style="margin-top:16px">{_esc(ms.jp_market_summary)}<br>{_esc(ms.growth_market_summary)}</div>'

        return f"""<div class="section">
  <div class="section-title">🌏 マーケット概況</div>
  <table>
    <thead><tr><th>指数</th><th class="num">終値</th><th class="num">週間</th><th class="num">月初来</th><th class="num">年初来</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
  {memo}
</div>"""

    def _candlestick_chart(self, daily_bars: list, weekly_bars: list, monthly_bars: list) -> str:
        """Google風インタラクティブチャート（日足/週足/月足切り替え・ホバーで株価表示）"""
        if not daily_bars or len(daily_bars) < 2:
            return '<div class="callout neutral" style="text-align:center">日足データなし</div>'

        import json
        def _serialize(bars):
            return json.dumps([{
                "d": b.date, "o": round(b.open, 1), "h": round(b.high, 1),
                "l": round(b.low, 1), "c": round(b.close, 1), "v": b.volume
            } for b in bars])

        daily_json = _serialize(daily_bars)
        weekly_json = _serialize(weekly_bars) if weekly_bars and len(weekly_bars) >= 2 else "[]"
        monthly_json = _serialize(monthly_bars) if monthly_bars and len(monthly_bars) >= 2 else "[]"
        chart_id = f"sc_{id(daily_bars) % 100000}"

        return f"""
<div id="{chart_id}" style="position:relative;width:100%;user-select:none">
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
    <div style="display:inline-flex;background:#eee;border-radius:6px;padding:2px;gap:2px" id="{chart_id}_tabs">
      <button data-mode="daily" style="padding:4px 14px;border:none;border-radius:5px;font-size:12px;font-weight:600;cursor:pointer;background:{NAVY};color:#fff">日足</button>
      <button data-mode="weekly" style="padding:4px 14px;border:none;border-radius:5px;font-size:12px;font-weight:600;cursor:pointer;background:transparent;color:{GRAY}">週足</button>
      <button data-mode="monthly" style="padding:4px 14px;border:none;border-radius:5px;font-size:12px;font-weight:600;cursor:pointer;background:transparent;color:{GRAY}">月足</button>
    </div>
    <div id="{chart_id}_tip" style="flex:1;font-size:12px;color:{NAVY};line-height:1.5;min-height:20px"></div>
  </div>
  <div style="position:relative">
    <canvas id="{chart_id}_cv" style="width:100%;cursor:crosshair"></canvas>
    <div id="{chart_id}_vline" style="display:none;position:absolute;top:0;width:1px;background:rgba(40,75,125,0.25);pointer-events:none"></div>
  </div>
</div>
<script>
(function(){{
  var datasets={{daily:{daily_json},weekly:{weekly_json},monthly:{monthly_json}}};
  var mode="daily";
  var D=datasets.daily;
  var cv=document.getElementById("{chart_id}_cv");
  var tip=document.getElementById("{chart_id}_tip");
  var vline=document.getElementById("{chart_id}_vline");
  var tabs=document.getElementById("{chart_id}_tabs");
  var dpr=window.devicePixelRatio||1;

  // Tab switching
  tabs.querySelectorAll("button").forEach(function(btn){{
    btn.addEventListener("click",function(){{
      var m=this.getAttribute("data-mode");
      if(m===mode||!datasets[m]||datasets[m].length<2) return;
      mode=m; D=datasets[m];
      tabs.querySelectorAll("button").forEach(function(b){{
        b.style.background=b.getAttribute("data-mode")===mode?"{NAVY}":"transparent";
        b.style.color=b.getAttribute("data-mode")===mode?"#fff":"{GRAY}";
      }});
      tip.innerHTML="";
      resize();
    }});
  }});

  function resize(){{
    var w=cv.parentElement.clientWidth;
    cv.width=w*dpr; cv.height=300*dpr;
    cv.style.height="300px";
    draw(-1);
  }}
  function fmt(v){{ return "¥"+v.toLocaleString("ja-JP",{{maximumFractionDigits:0}}) }}
  function fmtV(v){{ return v>=1e6?(v/1e6).toFixed(1)+"M":v>=1e3?(v/1e3).toFixed(0)+"K":v.toString() }}

  function draw(hi){{
    var ctx=cv.getContext("2d");
    var W=cv.width, H=cv.height;
    var padL=55*dpr, padR=10*dpr, padT=10*dpr;
    var priceH=210*dpr, volH=55*dpr, gapH=10*dpr;
    var cW=W-padL-padR, n=D.length;
    ctx.clearRect(0,0,W,H);
    // ranges
    var pMax=-Infinity,pMin=Infinity,vMax=0;
    for(var i=0;i<n;i++){{ if(D[i].h>pMax)pMax=D[i].h; if(D[i].l<pMin)pMin=D[i].l; if(D[i].v>vMax)vMax=D[i].v; }}
    pMax*=1.01; pMin*=0.99; var pR=pMax-pMin||1; vMax=vMax||1;
    function py(p){{ return padT+(1-(p-pMin)/pR)*priceH; }}
    function vy(v){{ return padT+priceH+gapH+volH-(v/vMax)*volH; }}
    var bW=Math.max(3*dpr,Math.min(14*dpr,(cW/n)-2*dpr));
    var gap=(cW-bW*n)/Math.max(n-1,1);
    // grid
    ctx.strokeStyle="#eee"; ctx.lineWidth=1*dpr;
    ctx.fillStyle="{GRAY}"; ctx.font=(10*dpr)+"px -apple-system,sans-serif"; ctx.textAlign="right";
    for(var g=0;g<5;g++){{
      var gy=padT+g*priceH/4; var gp=pMax-g*pR/4;
      ctx.beginPath(); ctx.moveTo(padL,gy); ctx.lineTo(W-padR,gy); ctx.stroke();
      ctx.fillText(fmt(gp),padL-4*dpr,gy+4*dpr);
    }}
    // volume avg line
    var avgV=0; for(var i=0;i<n;i++) avgV+=D[i].v; avgV/=n;
    var avy=vy(avgV);
    ctx.setLineDash([4*dpr,3*dpr]); ctx.strokeStyle="{BLUE}"; ctx.globalAlpha=0.4;
    ctx.beginPath(); ctx.moveTo(padL,avy); ctx.lineTo(W-padR,avy); ctx.stroke();
    ctx.setLineDash([]); ctx.globalAlpha=1;
    ctx.fillStyle="{BLUE}"; ctx.font=(9*dpr)+"px -apple-system,sans-serif"; ctx.textAlign="end";
    var avgLabel=mode==="daily"?"日平均":mode==="weekly"?"週平均":"月平均";
    ctx.fillText(avgLabel+" "+fmtV(avgV),W-padR,avy-3*dpr);
    // candles + volume
    for(var i=0;i<n;i++){{
      var b=D[i]; var cx=padL+i*(bW+gap)+bW/2;
      var up=b.c>=b.o; var col=up?"{RED}":"{GREEN}";
      // volume bar
      var vt=vy(b.v), vb=vy(0);
      ctx.fillStyle=up?"rgba(40,75,125,0.25)":"rgba(40,75,125,0.12)";
      ctx.fillRect(cx-bW/2,vt,bW,vb-vt);
      // wick
      ctx.strokeStyle=col; ctx.lineWidth=1*dpr;
      ctx.beginPath(); ctx.moveTo(cx,py(b.h)); ctx.lineTo(cx,py(b.l)); ctx.stroke();
      // body
      var yt=py(Math.max(b.o,b.c)), yb=py(Math.min(b.o,b.c));
      var bh=Math.max(yb-yt,1*dpr);
      if(up){{ ctx.fillStyle="#fff"; ctx.fillRect(cx-bW/2,yt,bW,bh); ctx.strokeRect(cx-bW/2,yt,bW,bh); }}
      else{{ ctx.fillStyle=col; ctx.fillRect(cx-bW/2,yt,bW,bh); }}
      // highlight
      if(i===hi){{
        ctx.fillStyle="rgba(40,75,125,0.06)";
        ctx.fillRect(cx-bW/2-2*dpr,padT,bW+4*dpr,priceH+gapH+volH);
      }}
      // date labels
      var showL=(n<=15)||(i%Math.max(1,Math.floor(n/8))===0)||(i===n-1);
      if(showL){{
        ctx.fillStyle="{GRAY}"; ctx.font=(9*dpr)+"px -apple-system,sans-serif"; ctx.textAlign="center";
        ctx.fillText(b.d,cx,padT+priceH+gapH+volH+14*dpr);
      }}
    }}
  }}
  // hover
  cv.addEventListener("mousemove",function(e){{
    var rect=cv.getBoundingClientRect();
    var mx=(e.clientX-rect.left)*dpr;
    var padL=55*dpr, padR=10*dpr, cW=cv.width-padL-padR, n=D.length;
    var bW=Math.max(3*dpr,Math.min(14*dpr,(cW/n)-2*dpr));
    var gap=(cW-bW*n)/Math.max(n-1,1);
    var idx=Math.floor((mx-padL)/(bW+gap));
    if(idx<0||idx>=n){{ tip.innerHTML=""; vline.style.display="none"; draw(-1); return; }}
    var b=D[idx]; var chg=b.c-b.o, chgPct=b.o?((chg/b.o)*100):0;
    var sign=chg>=0?"+":""; var col=chg>=0?"{RED}":"{GREEN}";
    tip.innerHTML='<span style="font-weight:700">'+b.d+'</span>　'+
      '<span style="color:{GRAY}">始</span> '+fmt(b.o)+
      '　<span style="color:{GRAY}">高</span> '+fmt(b.h)+
      '　<span style="color:{GRAY}">安</span> '+fmt(b.l)+
      '　<span style="color:{GRAY}">終</span> <b style="color:'+col+'">'+fmt(b.c)+'</b>'+
      '　<span style="color:'+col+'">'+sign+chg.toFixed(0)+' ('+sign+chgPct.toFixed(1)+'%)</span>'+
      '　<span style="color:{GRAY}">出来高</span> '+fmtV(b.v);
    var rx=(e.clientX-rect.left);
    vline.style.left=rx+"px"; vline.style.display="block"; vline.style.height=cv.style.height;
    draw(idx);
  }});
  cv.addEventListener("mouseleave",function(){{ tip.innerHTML=""; vline.style.display="none"; draw(-1); }});
  window.addEventListener("resize",resize);
  resize();
}})();
</script>"""

    def _section_tential_performance(self):
        t = self.q.tential
        rng = f"¥{t.weekly_low:,.0f} — ¥{t.weekly_high:,.0f}" if t.weekly_high else "N/A"
        tv = f"¥{t.avg_turnover / 1e6:,.0f}M" if t.avg_turnover else "N/A"
        tv_chg = _fp(t.turnover_change) if t.avg_turnover else "N/A"
        avg_vol_fmt = f"{t.avg_volume:,}" if t.avg_volume else "N/A"

        wc = _pcolor(t.weekly_return)
        mc = _pcolor(t.mtd_return)
        yc = _pcolor(t.ytd_return)

        # Candlestick chart
        chart_html = self._candlestick_chart(t.daily_bars, t.weekly_bars, t.monthly_bars)

        minkabu = _minkabu_url("325A")

        return f"""<div class="section">
  <div class="section-title">📈 <a href="{minkabu}" target="_blank" style="color:inherit;text-decoration:none">TENTIAL（325A）パフォーマンス</a></div>
  <div class="kpi-row">
    <div class="kpi">
      <div class="kpi-label">終値</div>
      <div class="kpi-value">¥{t.close:,.0f}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">週間</div>
      <div class="kpi-value" style="color:{wc}">{_fp(t.weekly_return)}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">月初来</div>
      <div class="kpi-value" style="color:{mc}">{_fp(t.mtd_return)}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">年初来</div>
      <div class="kpi-value" style="color:{yc}">{_fp(t.ytd_return)}</div>
    </div>
  </div>
  <div style="margin:16px 0;background:#f8f7f5;border-radius:8px;padding:16px;overflow-x:auto">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
      <span style="font-size:13px;font-weight:600;color:{NAVY}">日足チャート（OHLCV）</span>
      <div style="display:flex;gap:16px;font-size:11px;color:{GRAY}">
        <span>週間レンジ: {rng}</span>
        <span>週平均出来高: <strong>{avg_vol_fmt}</strong></span>
      </div>
    </div>
    {chart_html}
  </div>
  <div class="kpi-row">
    <div class="kpi">
      <div class="kpi-label">週間レンジ</div>
      <div class="kpi-value" style="font-size:18px">{rng}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">売買代金（週平均）</div>
      <div class="kpi-value" style="font-size:18px">{tv}</div>
      <div class="kpi-sub" style="color:{_pcolor(t.turnover_change)}">前週比 {tv_chg}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">出来高（週平均）</div>
      <div class="kpi-value" style="font-size:18px">{avg_vol_fmt}</div>
    </div>
  </div>
</div>"""

    def _section_comps_heatmap(self):
        """Heatmap table: rows = stocks, cols = weekly/mtd/ytd"""
        all_stocks = [self.q.tential] + self.q.comps
        rows = ""
        current_cat = ""
        for s in all_stocks:
            cat_label = ""
            if s.category != current_cat:
                current_cat = s.category
                cat_label = _esc(current_cat)

            star = " ⭐" if s.code == "325A" else ""
            tr_class = ' class="star"' if s.code == "325A" else ""

            w_bg = _heat_bg(s.weekly_return)
            m_bg = _heat_bg(s.mtd_return)
            y_bg = _heat_bg(s.ytd_return, 15.0)

            close_fmt = f"¥{s.close:,.0f}" if s.close > 100 else f"${s.close:,.2f}"
            vol_fmt = f"{s.avg_volume:,}" if s.avg_volume else "—"
            per_fmt = f"{s.per:.1f}x" if s.per else "—"
            per_color = RED if s.per and s.per > 40 else (GREEN if s.per and s.per < 15 else GRAY)

            rows += f"""<tr{tr_class}>
  <td class="cat-label">{cat_label}</td>
  <td>{_stock_link(s.name, s.code, star)}</td>
  <td class="num">{close_fmt}</td>
  <td class="num" style="background:{w_bg};color:{_pcolor(s.weekly_return)}">{_fp(s.weekly_return)}</td>
  <td class="num" style="background:{m_bg};color:{_pcolor(s.mtd_return)}">{_fp(s.mtd_return)}</td>
  <td class="num" style="background:{y_bg};color:{_pcolor(s.ytd_return)}">{_fp(s.ytd_return)}</td>
  <td class="num" style="font-size:12px;color:{per_color};font-weight:600">{per_fmt}</td>
  <td class="num" style="font-size:11px;color:{GRAY}">{vol_fmt}</td>
</tr>"""

        return f"""<div class="section">
  <div class="section-title">🔥 Comps ヒートマップ</div>
  <div style="overflow-x:auto">
  <table class="heatmap">
    <thead><tr><th>カテゴリ</th><th>銘柄</th><th class="num">終値</th><th class="num">週間</th><th class="num">月初来</th><th class="num">年初来</th><th class="num">PER</th><th class="num">出来高(週平均)</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
  </div>
</div>"""

    def _section_comps_ranking(self):
        all_stocks = [self.q.tential] + self.q.comps
        ranked = sorted(all_stocks, key=lambda x: x.weekly_return, reverse=True)

        # Find max absolute return for bar scaling
        max_abs = max(abs(s.weekly_return) for s in ranked) if ranked else 1
        if max_abs == 0:
            max_abs = 1

        items = ""
        for i, s in enumerate(ranked, 1):
            if i <= 3:
                rank_class = f"rank-{i}"
            else:
                rank_class = "rank-other"
            star = " ⭐" if s.code == "325A" else ""
            val_color = _pcolor(s.weekly_return)
            bar_w = abs(s.weekly_return) / max_abs * 100
            bar_color = RED if s.weekly_return > 0 else GREEN

            minkabu = _minkabu_url(s.code)
            items += f"""<div class="rank-item">
  <div class="rank-num {rank_class}">{i}</div>
  <div class="rank-name"><a href="{minkabu}" target="_blank" style="color:inherit;text-decoration:none">{_esc(s.name)}{star}</a> <span class="code">{_esc(s.code)}</span></div>
  <div class="rank-bar"><div class="rank-bar-fill" style="width:{bar_w:.0f}%;background:{bar_color}"></div></div>
  <div class="rank-value" style="color:{val_color}">{_fp(s.weekly_return)}</div>
</div>"""

        return f"""<div class="section">
  <div class="section-title">📊 週間騰落率ランキング</div>
  {items}
</div>"""

    def _section_category_summary(self):
        rows = ""
        for cp in self.q.categories:
            wc = _pcolor(cp.avg_weekly_return)
            mc = _pcolor(cp.avg_mtd_return)
            yc = _pcolor(cp.avg_ytd_return)
            rows += f"""<tr>
  <td><strong>{_esc(cp.category)}</strong></td>
  <td class="num" style="color:{wc}">{_fp(cp.avg_weekly_return)}</td>
  <td class="num" style="color:{mc}">{_fp(cp.avg_mtd_return)}</td>
  <td class="num" style="color:{yc}">{_fp(cp.avg_ytd_return)}</td>
  <td>{_esc(cp.best_performer)}</td>
  <td>{_esc(cp.worst_performer)}</td>
</tr>"""

        return f"""<div class="section">
  <div class="section-title">🏷️ カテゴリ別サマリ</div>
  <table>
    <thead><tr><th>カテゴリ</th><th class="num">週間平均</th><th class="num">月初来</th><th class="num">年初来</th><th>Best</th><th>Worst</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""

    def _section_margin_analysis(self):
        m = self.q.margin
        if not m.long_balance and not m.short_balance:
            return f"""<div class="section">
  <div class="section-title">⚖️ 需給分析（TENTIAL 信用残）</div>
  <div class="callout neutral">信用残データは取得後に自動表示されます。</div>
</div>"""

        lc = _pcolor(m.long_change_pct)
        sc = _pcolor(-m.short_change_pct)  # short increase is bearish signal
        ratio_emoji = "⚠️" if m.ratio > 5 else ("📊" if m.ratio > 2 else "✅")

        return f"""<div class="section">
  <div class="section-title">⚖️ 需給分析（TENTIAL 信用残）</div>
  <div class="margin-grid">
    <div class="margin-card">
      <h4>信用買い残</h4>
      <div class="val">{m.long_balance:,} 株</div>
      <div class="chg" style="color:{lc}">前週比 {_fp(m.long_change_pct)}</div>
    </div>
    <div class="margin-card">
      <h4>信用売り残</h4>
      <div class="val">{m.short_balance:,} 株</div>
      <div class="chg" style="color:{sc}">前週比 {_fp(m.short_change_pct)}</div>
    </div>
    <div class="margin-card" style="grid-column:1/-1">
      <h4>{ratio_emoji} 貸借倍率</h4>
      <div class="val">{m.ratio:.2f}x</div>
      <div class="chg" style="color:var(--gray)">{"買い残が売り残の" + f"{m.ratio:.1f}倍" if m.ratio > 1 else "売り残優位"}</div>
    </div>
  </div>
  <div style="margin-top:12px;font-size:11px;color:{GRAY}">出典: <a href="https://kabutan.jp/stock/kabuka?code=325A&ashi=shin" target="_blank" style="color:{BLUE};text-decoration:none">株探 信用残</a> | <a href="https://www.jpx.co.jp/markets/statistics-equities/short-selling/" target="_blank" style="color:{BLUE};text-decoration:none">JPX 空売り残高</a></div>
</div>"""

    def _news_list_html(self, news_items, max_items=5):
        """ニュースアイテムのリストHTML生成"""
        if not news_items:
            return '<p style="color:var(--gray);font-size:13px">ニュースなし</p>'
        items = ""
        for n in news_items[:max_items]:
            source_tag = f'<span style="color:var(--gray);font-size:11px">{_esc(n.source)}</span>' if n.source else ""
            # If news item has a URL, make title clickable
            url = getattr(n, 'url', '') or getattr(n, 'link', '')
            if url:
                title_html = f'<a href="{_esc(url)}" target="_blank" style="color:inherit;text-decoration:none;border-bottom:1px dotted {GRAY}">{_esc(n.title)}</a>'
            else:
                title_html = _esc(n.title)
            items += f'<div style="padding:8px 0;border-bottom:1px solid #f0f0f0;font-size:13px;line-height:1.5">{title_html} {source_tag}</div>'
        return items

    def _section_qualitative(self):
        yb = self.ql.yahoo_bbs
        ms = self.ql.market_sentiment

        # Detect stub mode
        is_stub = not ms.market_news and not ms.jp_market_summary

        if is_stub:
            return f"""<div class="section">
  <div class="section-title">💬 定性分析（マーケットインテリジェンス）</div>
  <div class="callout neutral" style="text-align:center;padding:32px">
    <div style="font-size:32px;margin-bottom:12px">🔄</div>
    <strong>定性データは定期実行時に自動収集されます</strong>
  </div>
</div>"""

        # Market summary callout
        summaries = []
        if ms.jp_market_summary:
            summaries.append(ms.jp_market_summary)
        if ms.growth_market_summary:
            summaries.append(ms.growth_market_summary)
        if ms.global_summary:
            summaries.append(ms.global_summary)
        summary_html = "<br>".join(_esc(s) for s in summaries)

        # Sentiment badge
        sent_html = ""
        if yb.post_count > 0:
            badge_color = RED if yb.bullish_pct > yb.bearish_pct else (GREEN if yb.bearish_pct > yb.bullish_pct else GRAY)
            sent_html = f"""<div style="display:flex;gap:16px;align-items:center;margin-top:12px;padding:12px;background:#f8f7f5;border-radius:8px">
  <div style="font-size:13px;color:var(--gray)">センチメント</div>
  <div style="display:flex;gap:8px">
    <span style="background:rgba(201,68,68,0.1);color:{RED};padding:3px 10px;border-radius:12px;font-size:12px;font-weight:600">強気 {yb.bullish_pct:.0f}%</span>
    <span style="background:#f0f0f0;color:var(--gray);padding:3px 10px;border-radius:12px;font-size:12px;font-weight:600">中立 {yb.neutral_pct:.0f}%</span>
    <span style="background:rgba(46,139,87,0.1);color:{GREEN};padding:3px 10px;border-radius:12px;font-size:12px;font-weight:600">弱気 {yb.bearish_pct:.0f}%</span>
  </div>
  <div style="font-size:13px;font-weight:600;color:{badge_color}">→ {_esc(yb.trend)}</div>
</div>"""

        # Topic category analysis
        topic_html = ""
        topics = yb.topic_categories
        if topics:
            # Bar chart visualization
            max_count = max(tc.count for tc in topics) if topics else 1
            topic_emojis = {
                "業績・決算": "📊", "株価・テクニカル": "📈", "需給・信用残": "⚖️",
                "事業・経営": "🏢", "マクロ・市場環境": "🌍", "投資判断・売買": "💰", "その他": "💭",
            }
            bars_html = ""
            for tc in topics:
                emoji = topic_emojis.get(tc.name, "📌")
                bar_w = tc.count / max_count * 100
                # Sample posts tooltip
                samples = " / ".join(_esc(s) for s in tc.sample_posts[:2])
                bars_html += f"""<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">
  <div style="width:130px;font-size:12px;font-weight:600;color:{NAVY};white-space:nowrap">{emoji} {_esc(tc.name)}</div>
  <div style="flex:1;background:#eee;border-radius:4px;height:20px;overflow:hidden">
    <div style="width:{bar_w:.0f}%;height:100%;background:linear-gradient(90deg,{NAVY},{BLUE});border-radius:4px;transition:width 0.3s"></div>
  </div>
  <div style="width:70px;text-align:right;font-size:12px;font-weight:700;color:{NAVY}">{tc.count}件 ({tc.pct:.0f}%)</div>
</div>
<div style="margin:-2px 0 8px 140px;font-size:11px;color:{GRAY};line-height:1.4">{samples}</div>"""

            topic_html = f"""<div class="qual-card" style="grid-column:1/-1">
  <h4>📋 掲示板コメント — トピック分析</h4>
  <div style="font-size:12px;color:{GRAY};margin-bottom:12px">直近3日間の投稿 {yb.post_count}件 をトピック別に分類（1投稿が複数カテゴリに該当する場合あり）</div>
  {bars_html}
</div>"""

        # Forum notable posts
        forum_html = ""
        notable = yb.notable_comments
        if notable:
            posts_items = ""
            for comment in notable[:5]:
                posts_items += f'<div style="padding:8px 12px;background:#fff;border-radius:6px;margin-bottom:6px;font-size:12px;line-height:1.5;border-left:3px solid {BLUE}">{_esc(comment)}</div>'
            forum_html = f"""<div class="qual-card" style="grid-column:1/-1">
  <h4>🗣️ Yahoo! Finance 掲示板 — 注目投稿</h4>
  <div style="display:flex;gap:12px;margin-bottom:10px;font-size:12px;color:{GRAY}">
    <span>投稿数: <strong>{yb.post_count}</strong>件（直近3日）</span>
    <span>温度感: <strong style="color:{RED if yb.bullish_pct > yb.bearish_pct else (GREEN if yb.bearish_pct > yb.bullish_pct else GRAY)}">{_esc(yb.trend)}</strong></span>
  </div>
  {posts_items}
  <div style="margin-top:8px;font-size:11px;color:{GRAY}">出典: <a href="https://finance.yahoo.co.jp/quote/325A.T/forum" target="_blank" style="color:{BLUE};text-decoration:none">Yahoo! ファイナンス掲示板</a></div>
</div>"""

        # TENTIAL news (from Google News)
        tential_html = ""
        if ms.tential_news:
            tential_html = f"""<div class="qual-card">
  <h4>🏢 TENTIAL / BAKUNE 関連ニュース</h4>
  {self._news_list_html(ms.tential_news, 4)}
</div>"""

        # Market news
        market_html = ""
        if ms.market_news:
            market_html = f"""<div class="qual-card">
  <h4>📊 マーケット全体</h4>
  {self._news_list_html(ms.market_news, 5)}
</div>"""

        # Sector news
        sector_html = ""
        if ms.sector_news:
            sector_html = f"""<div class="qual-card" style="grid-column:1/-1">
  <h4>🏷️ 関連セクター（D2C・スポーツ・ウェルネス）</h4>
  {self._news_list_html(ms.sector_news, 5)}
</div>"""

        return f"""<div class="section">
  <div class="section-title">💬 マーケットインテリジェンス</div>
  <div class="callout">{summary_html}</div>
  {sent_html}
  <div class="qual-grid" style="margin-top:16px">
    {topic_html}
    {forum_html}
    {market_html}
    {tential_html}
    {sector_html}
  </div>
</div>"""

    def _section_watchpoints(self):
        events = self.ql.market_sentiment.key_events_next_week
        # Filter out stub text
        real_events = [e for e in events if "自動取得" not in e and "スケジュール" not in e]
        items = ""
        icons = ["📅", "🏛️", "📊", "💹", "⚡"]
        for i, ev in enumerate(real_events):
            icon = icons[i % len(icons)]
            items += f'<div class="watch-item"><span class="watch-icon">{icon}</span><span>{_esc(ev)}</span></div>'
        if not items:
            return ""  # Hide section entirely when no real data

        return f"""<div class="section">
  <div class="section-title">👁️ 来週のウォッチポイント</div>
  {items}
</div>"""


def generate_html_report(quant: QuantReport, qual: QualReport, metadata: dict, output_dir: str = None) -> str:
    """Generate HTML report and save to file. Returns file path."""
    gen = HTMLReportGenerator(quant, qual, metadata)
    html_content = gen.generate()

    if output_dir is None:
        output_dir = str(Path(__file__).resolve().parent.parent / "output")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    ws = metadata.get("week_start", datetime.date.today().isoformat())
    filename = f"weekly_report_{ws.replace('-', '')}.html"
    filepath = Path(output_dir) / filename
    filepath.write_text(html_content, encoding="utf-8")

    logger.info(f"📄 HTML report saved: {filepath}")
    return str(filepath)
