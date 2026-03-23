#!/usr/bin/env python3
"""
Standalone runner: injects Chrome-scraped stock data directly into the pipeline.
Bypasses data_fetcher (blocked by VM proxy) and runs Steps 2-6.
Date: 2026-03-23
"""
import sys, os, datetime, logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")
sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("run_with_data")

# ── Hardcoded data collected via Chrome from kabutan.jp on 2026-03-23 ──────────
# week_end = 2026-03-20 (Fri), prev_week_end = 2026-03-14 (Fri)
# MTD baseline = 2026-02-28 close

RAW = {
    # code: (name, category, close, weekRet, mtd, wHigh, wLow, avgVol, per, spark20)
    "325A": ("TENTIAL",   "自社",           3290,  -7.82, -24.11, 3770,   3245,  111600,  13.2,
             [4125,4120,4210,4185,4335,4200,3925,3690,3830,3945,3790,3890,3910,3775,3770,3675,3615,3770,3475,3290]),
    "4933": ("I-ne",      "Domestic EC",    1056,   2.77,  -6.22, 1113,   1049,  191345,   9.3,
             [1101,1120,1071,1112,1126,1087,1085,1032,1085,1077,1053,1063,1073,1042,1048,1080,1094,1109,1077,1056]),
    "2930": ("北の達人",   "Domestic EC",    122,   -1.60, -10.29,  126,    122,  785375,  27.8,
             [137,137,136,135,136,131,128,126,129,126,123,126,124,123,125,124,124,124,123,122]),
    "3182": ("Oisix",     "Domestic EC",    1325,  -1.77,  -7.41, 1381,   1291,  136860,  11.8,
             [1385,1383,1382,1408,1431,1405,1385,1355,1378,1399,1358,1384,1386,1370,1354,1349,1354,1373,1330,1325]),
    "7936": ("アシックス", "Sports Apparel", 4221,  -1.26, -11.95, 4600,   4180, 3761905,  28.4,
             [4810,4798,4670,4765,4794,4843,4608,4654,4560,4607,4373,4331,4486,4425,4460,4353,4402,4565,4404,4221]),
    "8111": ("ゴールドウイン","Sports Apparel",2281,  0.15,  -7.54, 2413.5, 2279,  494875,  12.7,
             [2420,2450,2431,2423.5,2467,2423,2355,2296.5,2318,2359.5,2361,2347,2368.5,2323.5,2330,2319,2360.5,2413.5,2333.5,2281]),
    "7806": ("MTG",       "Wellness",       5800,  -1.75,   0.87, 6410,   5800,  197950,  25.3,
             [5270,5230,5170,5270,5750,5670,5340,5670,5960,5800,5860,6070,6350,6290,6270,6270,6110,6410,6160,5800]),
    "6630": ("ヤーマン",  "Wellness",        688,   1.47,  -2.96,  695,    661,  116785, 104.0,
             [705,705,700,713,709,704,697,687,687,685,678,691,696,686,681,661,665,693,691,688]),
    "ONON": ("On Holdings","Global D2C",   37.66,  -0.87, -18.98, 40.56,  37.30,       0,  51.7,
             [47.44,46.79,46.65,47.60,46.48,46.76,43.91,43.23,43.37,41.46,41.18,41.02,39.44,37.96,37.99,39.25,39.79,39.05,38.87,37.66]),
    "LULU": ("lululemon", "Global D2C",   162.82,   3.19, -12.07, 169.40, 156.78,      0,  11.1,
             [178.11,179.49,182.55,186.10,185.17,176.17,174.27,173.21,173.18,170.13,169.76,166.43,162.79,158.19,157.78,159.90,162.47,158.78,157.55,162.82]),
    "FIGS": ("FIGS",      "Global D2C",    14.21,  -4.31,  -8.03, 15.19,  14.05,       0,  75.8,
             [15.54,15.80,15.93,16.10,15.68,15.07,14.96,14.97,14.77,14.91,14.68,14.67,14.48,14.48,14.66,14.82,14.64,14.60,14.47,14.21]),
    "YETI": ("YETI",      "Global D2C",    35.50,  -2.66, -18.78, 37.26,  35.18,       0,  18.5,
             [42.61,43.01,42.64,42.35,42.27,41.14,41.00,40.63,39.97,39.58,39.59,39.29,38.57,37.67,37.21,37.30,37.68,36.66,36.65,35.50]),
    "OLPX": ("Olaplex",  "Global D2C",     1.33,  10.83, -17.39,  1.41,   1.19,        0,  12.5,
             [1.54,1.57,1.54,1.53,1.52,1.48,1.48,1.47,1.42,1.42,1.42,1.42,1.41,1.33,1.31,1.30,1.30,1.29,1.26,1.33]),
    "247A": ("AIロボティクス","IPO Peers",  1219,  -3.00,  -4.02, 1325,   1193, 1364180,  24.2,
             [1287,1292,1246,1286,1270,1223,1170,1086,1165,1313,1283,1335,1435,1365,1300,1277,1275,1320,1261,1219]),
    "5892": ("yutori",    "IPO Peers",      1984,  -6.42, -15.14, 2310,   1951,   71990,  28.4,
             [2256,2080,2215,2251,2338,2265,2188,2101,2221,2350,2294,2286,2279,2187,2228,2258,2196,2254,2085,1984]),
    "456A": ("Human Made","IPO Peers",      4505,  22.54,  39.47, 4620,   3555,  190230,   None,
             [3395,3330,3390,3235,3230,3305,3285,3285,3595,3650,3475,3600,3605,3585,3550,3675,4220,4330,4350,4505]),
}

BENCHMARKS_RAW = {
    # name: (close, weekRet, mtd, spark20)
    "N225":      (51614.89, -0.83, -12.29,
                  [56680,56770,55400,54550,55200,56100,55000,54800,55600,56400,55200,55400,56200,55900,55100,54500,54800,55700,53200,51615]),
    "TOPIX":     (3501.42,  -0.54, -11.10,
                  [3901,3910,3822,3780,3820,3870,3800,3790,3820,3850,3780,3790,3830,3810,3760,3710,3740,3790,3620,3501]),
    "Growth250": (711.70,   -1.67,  -8.49,
                  [769,770,755,747,753,761,750,745,750,758,748,749,755,751,743,735,740,751,723,712]),
    "S&P 500":   (5577.47,  -2.27, -10.12,
                  [6050,6070,5970,5880,5930,5960,5880,5850,5870,5900,5820,5830,5880,5850,5780,5720,5740,5800,5640,5577]),
}

# ── Build QuantReport ──────────────────────────────────────────────────────────
from modules.quantitative import (
    QuantReport, StockPerformance, CategoryPerformance, MarginData, DailyBar
)
from config.settings import CATEGORY_ORDER

def make_perf(code, data_tuple):
    name, cat, close, wret, mtd, wHigh, wLow, avgVol, per, spark = data_tuple
    sp = StockPerformance(name=name, code=code, category=cat)
    sp.close        = close
    sp.weekly_return = wret
    sp.mtd_return   = mtd
    sp.ytd_return   = 0.0        # no full YTD data available
    sp.weekly_high  = wHigh
    sp.weekly_low   = wLow
    sp.avg_volume   = int(avgVol)
    sp.per          = per or 0.0
    # Build minimal daily_bars from spark (last 20 closes)
    today = datetime.date(2026, 3, 23)
    for i, c in enumerate(spark):
        db = DailyBar()
        db.date = str(today - datetime.timedelta(days=len(spark)-1-i))
        db.close = c
        db.open = db.high = db.low = c
        sp.daily_bars.append(db)
    return sp

tential_perf = make_perf("325A", RAW["325A"])
comps_list   = [make_perf(code, data) for code, data in RAW.items() if code != "325A"]

# Sort comps by category order
cat_idx = {c: i for i, c in enumerate(CATEGORY_ORDER)}
comps_list.sort(key=lambda x: (cat_idx.get(x.category, 99), x.weekly_return))

# Category summaries
from collections import defaultdict
cat_stocks = defaultdict(list)
for s in comps_list:
    cat_stocks[s.category].append(s)

categories = []
for cat in CATEGORY_ORDER:
    stocks = cat_stocks.get(cat, [])
    if not stocks:
        continue
    cp = CategoryPerformance(category=cat)
    cp.avg_weekly_return = round(sum(s.weekly_return for s in stocks) / len(stocks), 2)
    cp.avg_mtd_return    = round(sum(s.mtd_return    for s in stocks) / len(stocks), 2)
    cp.best_performer  = max(stocks, key=lambda s: s.weekly_return).name
    cp.worst_performer = min(stocks, key=lambda s: s.weekly_return).name
    categories.append(cp)

# Benchmarks
benchmarks = {}
for bname, (close, wret, mtd, spark) in BENCHMARKS_RAW.items():
    sp = StockPerformance(name=bname, code=bname, category="Benchmark")
    sp.close         = close
    sp.weekly_return = wret
    sp.mtd_return    = mtd
    sp.ytd_return    = 0.0
    today = datetime.date(2026, 3, 23)
    for i, c in enumerate(spark):
        db = DailyBar()
        db.date = str(today - datetime.timedelta(days=len(spark)-1-i))
        db.close = c
        sp.daily_bars.append(db)
    benchmarks[bname] = sp

quant_report = QuantReport(
    tential    = tential_perf,
    comps      = comps_list,
    categories = categories,
    benchmarks = benchmarks,
    margin     = MarginData(),
)

# ── Build QualReport (minimal — no qualitative data available without web access) ─
from modules.qualitative import QualReport
qual_report = QualReport()

# ── Metadata ─────────────────────────────────────────────────────────────────
metadata = {
    "week_start": "2026-03-16",
    "week_end":   "2026-03-20",
    "generated":  "2026-03-23",
    "note":       "株価データ: Kabutan (Chrome経由取得, 2026-03-23)"
}

# ── Step 4: Generate HTML ─────────────────────────────────────────────────────
from modules.html_report import generate_html_report
logger.info("📄 Generating HTML report…")
html_path = generate_html_report(quant_report, qual_report, metadata)
logger.info(f"   ✅ HTML: {html_path}")

# ── Step 5: Deploy to GitHub Pages ────────────────────────────────────────────
from modules.github_pages import deploy_to_pages, git_push_pages
from config.settings import GITHUB_PAGES_BASE_URL

logger.info("🌐 Deploying to GitHub Pages…")
pages_path = deploy_to_pages(html_path)
report_url = None
if pages_path and GITHUB_PAGES_BASE_URL:
    report_url = f"{GITHUB_PAGES_BASE_URL.rstrip('/')}/{pages_path}"
    logger.info(f"   URL: {report_url}")

pushed = git_push_pages()
if pushed:
    logger.info("   ✅ git push succeeded")
else:
    logger.warning("   ⚠️  git push failed — manual push needed")

# ── Step 6: Notion + Slack ────────────────────────────────────────────────────
from modules.notion_publisher import NotionPublisher
from modules.slack_publisher import SlackPublisher, build_summary_text
from config.settings import SLACK_BOT_TOKEN, SLACK_CHANNEL_ID

logger.info("📤 Publishing to Notion…")
publisher  = NotionPublisher()
notion_url = publisher.publish(quant_report, qual_report, metadata, pages_path=pages_path)
if notion_url:
    logger.info(f"   ✅ Notion: {notion_url}")
else:
    logger.warning("   ⚠️  Notion publish failed")

if SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
    logger.info("   Sending to Slack…")
    summary = build_summary_text(quant_report, report_url=report_url)
    slack   = SlackPublisher()
    ts      = slack.post_report(summary, report_url=report_url, notion_url=notion_url)
    if ts:
        logger.info("   ✅ Slack delivered!")
    else:
        logger.warning("   ⚠️  Slack delivery failed")

logger.info("=" * 50)
logger.info(f"🎉 Done!  HTML → {html_path}")
if report_url:
    logger.info(f"         Web  → {report_url}")
if notion_url:
    logger.info(f"         Notion→ {notion_url}")
logger.info("=" * 50)
