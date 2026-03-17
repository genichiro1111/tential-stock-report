#!/usr/bin/env python3
"""
TENNIAL Weekly Stock Report — Main Entry Point
Usage:
  python main.py              # Full run (fetch → analyze → HTML → GitHub Pages → Notion → Slack)
  python main.py --dry-run    # Fetch & analyze only, skip publish
  python main.py --test       # Quick connectivity test
  python main.py --slack-only # Re-send latest report to Slack (skip data fetch)
"""
import argparse, datetime, logging, sys, json
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import (JQUANTS_API_KEY, NOTION_API_KEY,
                              NOTION_PARENT_PAGE_ID, NOTION_DATABASE_ID,
                              SLACK_BOT_TOKEN, SLACK_CHANNEL_ID,
                              GITHUB_PAGES_BASE_URL,
                              TENTIAL, COMPS, BENCHMARKS)
from modules.data_fetcher import StockDataFetcher
from modules.quantitative import QuantitativeAnalyzer
from modules.qualitative import QualitativeAnalyzer
from modules.notion_publisher import NotionPublisher
from modules.html_report import generate_html_report
from modules.slack_publisher import SlackPublisher, build_summary_text
from modules.github_pages import deploy_to_pages, git_push_pages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("weekly-report")


def check_config():
    """Validate required configuration."""
    errors = []
    if not JQUANTS_API_KEY:
        errors.append("JQUANTS_API_KEY is not set")
    if not NOTION_API_KEY:
        errors.append("NOTION_API_KEY is not set")
    if not NOTION_PARENT_PAGE_ID and not NOTION_DATABASE_ID:
        errors.append("Neither NOTION_PARENT_PAGE_ID nor NOTION_DATABASE_ID is set")
    if errors:
        for e in errors:
            logger.error(f"❌ Config error: {e}")
        logger.error("Please check your .env file")
        return False
    logger.info("✅ Configuration OK")
    logger.info(f"   J-Quants API Key: {JQUANTS_API_KEY[:8]}... (V2 x-api-key)")
    logger.info(f"   Notion API Key: {NOTION_API_KEY[:12]}...")
    logger.info(f"   Notion DB ID: {NOTION_DATABASE_ID or '(will auto-create)'}")
    logger.info(f"   TENTIAL: {TENTIAL.code} | Comps: {len(COMPS)} | Benchmarks: {len(BENCHMARKS)}")
    return True


def test_connectivity():
    """Quick test: check API connectivity."""
    import requests

    logger.info("=== Connectivity Test ===")

    # J-Quants V2 (x-api-key auth)
    try:
        r = requests.get(
            "https://api.jquants.com/v2/equities/bars/daily",
            headers={"x-api-key": JQUANTS_API_KEY},
            params={"code": TENTIAL.code + "0", "date": datetime.date.today().strftime("%Y%m%d")},
            timeout=10,
        )
        if r.status_code == 200:
            logger.info(f"✅ J-Quants V2 API: OK (status {r.status_code})")
        else:
            logger.warning(f"⚠️ J-Quants V2 API: status {r.status_code} — {r.text[:200]}")
    except Exception as e:
        logger.error(f"❌ J-Quants V2 API: {e}")

    # Notion
    try:
        r = requests.get(
            "https://api.notion.com/v1/users/me",
            headers={
                "Authorization": f"Bearer {NOTION_API_KEY}",
                "Notion-Version": "2022-06-28",
            },
            timeout=10,
        )
        if r.status_code == 200:
            logger.info(f"✅ Notion API: OK ({r.json().get('name', 'connected')})")
        else:
            logger.warning(f"⚠️ Notion API: status {r.status_code} — {r.text[:200]}")
    except Exception as e:
        logger.error(f"❌ Notion API: {e}")

    # yfinance (quick test)
    try:
        import yfinance as yf
        tk = yf.Ticker("^N225")
        info = tk.fast_info
        logger.info(f"✅ yfinance: OK (N225 last={getattr(info, 'last_price', 'N/A')})")
    except Exception as e:
        logger.error(f"❌ yfinance: {e}")

    logger.info("=== Test Complete ===")


def run_report(dry_run=False):
    """Execute full report pipeline."""
    logger.info("=" * 60)
    logger.info("📊 TENTIAL Weekly Stock Report")
    logger.info(f"   Date: {datetime.datetime.now():%Y-%m-%d %H:%M}")
    logger.info(f"   Mode: {'DRY RUN' if dry_run else 'FULL'}")
    logger.info("=" * 60)

    # Step 1: Fetch data
    logger.info("\n📥 Step 1/6: Fetching stock data...")
    fetcher = StockDataFetcher()
    data = fetcher.fetch_all()

    comp_count = len(data.get("comps", {}))
    bench_count = len(data.get("benchmarks", {}))
    logger.info(f"   TENTIAL: {len(data['tential'])} rows")
    logger.info(f"   Comps: {comp_count} stocks fetched")
    logger.info(f"   Benchmarks: {bench_count} indices fetched")

    if data["tential"].empty:
        logger.error("❌ Failed to fetch TENTIAL data. Aborting.")
        return None

    # Step 2: Quantitative analysis
    logger.info("\n📊 Step 2/6: Running quantitative analysis...")
    quant_analyzer = QuantitativeAnalyzer(data)
    quant_report = quant_analyzer.analyze()

    t = quant_report.tential
    logger.info(f"   TENTIAL Close: ¥{t.close:,.0f}")
    logger.info(f"   Weekly Return: {t.weekly_return:+.1f}%")
    logger.info(f"   MTD: {t.mtd_return:+.1f}% | YTD: {t.ytd_return:+.1f}%")

    g = quant_report.benchmarks.get("グロース250")
    if g:
        diff = t.weekly_return - g.weekly_return
        logger.info(f"   vs Growth250: {diff:+.1f}% ({'outperform' if diff > 0 else 'underperform'})")

    # Step 3: Qualitative analysis
    logger.info("\n💬 Step 3/6: Qualitative analysis...")
    qual_analyzer = QualitativeAnalyzer()
    qual_report = qual_analyzer.analyze()

    # Step 4: Generate HTML report
    logger.info("\n📄 Step 4/6: Generating HTML report...")
    html_path = generate_html_report(quant_report, qual_report, data["metadata"])
    logger.info(f"   HTML: {html_path}")

    # Step 5: Deploy to GitHub Pages
    pages_path = None
    report_url = None
    logger.info("\n🌐 Step 5/6: Deploying to GitHub Pages...")
    try:
        pages_path = deploy_to_pages(html_path)
        if pages_path:
            logger.info(f"   Deployed: {pages_path}")
            if GITHUB_PAGES_BASE_URL:
                report_url = f"{GITHUB_PAGES_BASE_URL.rstrip('/')}/{pages_path}"
                logger.info(f"   URL: {report_url}")
            if not dry_run:
                pushed = git_push_pages()
                if pushed:
                    logger.info("   ✅ GitHub Pages updated")
                else:
                    logger.info("   ⚠️ git push skipped (run manually)")
        else:
            logger.warning("   ⚠️ GitHub Pages deploy failed")
    except Exception as e:
        logger.warning(f"   ⚠️ GitHub Pages deploy error: {e}")

    # Step 6: Publish to Notion + Slack
    notion_url = None
    if dry_run:
        logger.info("\n🔒 Step 6/6: DRY RUN — skipping Notion & Slack publish")
        preview = {
            "tential": {"close": t.close, "weekly_return": t.weekly_return,
                        "mtd_return": t.mtd_return, "ytd_return": t.ytd_return},
            "comps": [{"name": c.name, "code": c.code, "weekly_return": c.weekly_return}
                      for c in quant_report.comps],
            "benchmarks": {k: {"close": v.close, "weekly_return": v.weekly_return}
                           for k, v in quant_report.benchmarks.items()},
            "metadata": data["metadata"],
        }
        preview_path = Path(__file__).parent / "last_preview.json"
        preview_path.write_text(json.dumps(preview, ensure_ascii=False, indent=2))
        logger.info(f"   Preview saved: {preview_path}")
    else:
        # Notion
        logger.info("\n📤 Step 6/6: Publishing to Notion & Slack...")
        publisher = NotionPublisher()
        notion_url = publisher.publish(quant_report, qual_report, data["metadata"], pages_path=pages_path)
        if notion_url:
            logger.info(f"   ✅ Notion: {notion_url}")
        else:
            logger.warning("   ⚠️ Notion publish failed (duplicate week or API error)")

        # Slack
        if SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
            logger.info("   Sending to Slack...")
            summary = build_summary_text(quant_report, report_url=report_url)
            slack = SlackPublisher()
            ts = slack.post_report(summary, report_url=report_url, notion_url=notion_url)
            if ts:
                logger.info("   ✅ Slack: delivered!")
            else:
                logger.warning("   ⚠️ Slack delivery failed")
        else:
            logger.info("   ⏭️ Slack not configured — skipping")

    logger.info("\n" + "=" * 60)
    logger.info("🎉 Report pipeline complete!")
    logger.info(f"   HTML: file://{html_path}")
    if report_url:
        logger.info(f"   Web:  {report_url}")
    if notion_url:
        logger.info(f"   Notion: {notion_url}")
    logger.info("=" * 60)
    return True


def resend_latest_to_slack():
    """最新レポートのサマリーをSlackに再送"""
    from config.settings import GITHUB_PAGES_BASE_URL
    output_dir = Path(__file__).parent / "output"
    htmls = sorted(output_dir.glob("weekly_report_*.html"), reverse=True)
    if not htmls:
        logger.error("No HTML reports found in output/")
        return False
    latest = htmls[0]
    report_url = None
    if GITHUB_PAGES_BASE_URL:
        report_url = f"{GITHUB_PAGES_BASE_URL.rstrip('/')}/reports/{latest.name}"
    logger.info(f"Re-sending: {latest.name}")
    slack = SlackPublisher()
    summary = f"📊 *TENTIAL Weekly Stock Report*（再送）\n{latest.stem}"
    if report_url:
        summary += f"\n\n👉 <{report_url}|詳細レポートはこちら>"
    ts = slack.post_report(summary, report_url=report_url)
    return ts is not None


def main():
    parser = argparse.ArgumentParser(description="TENTIAL Weekly Stock Report")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch & analyze only, skip publish")
    parser.add_argument("--test", action="store_true",
                        help="Quick API connectivity test")
    parser.add_argument("--slack-only", action="store_true",
                        help="Re-send latest report summary to Slack")
    args = parser.parse_args()

    if not check_config():
        sys.exit(1)

    if args.test:
        test_connectivity()
        return

    if args.slack_only:
        if not resend_latest_to_slack():
            sys.exit(1)
        return

    result = run_report(dry_run=args.dry_run)
    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
