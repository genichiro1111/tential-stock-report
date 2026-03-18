"""
Weekly Stock Report - Configuration
"""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_env_path)

# API Keys
JQUANTS_API_KEY = os.environ.get("JQUANTS_API_KEY", "")
JQUANTS_MAIL = os.environ.get("JQUANTS_MAIL", "")
JQUANTS_PASSWORD = os.environ.get("JQUANTS_PASSWORD", "")
NOTION_API_KEY = os.environ.get("NOTION_API_KEY", "")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "")
EDINETDB_API_KEY = os.environ.get("EDINETDB_API_KEY", "")  # https://edinetdb.jp/developers
NOTION_PARENT_PAGE_ID = os.environ.get("NOTION_PARENT_PAGE_ID", "")
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "")
GITHUB_PAGES_BASE_URL = os.environ.get("GITHUB_PAGES_BASE_URL", "")  # e.g. https://username.github.io/tential-stock-report

@dataclass
class CompanyInfo:
    name: str
    code: str
    market: str
    category: str
    source: str = "jquants"
    yfinance_ticker: str = ""

TENTIAL = CompanyInfo("TENTIAL", "325A", "グロース", "自社", "jquants")

COMPS: List[CompanyInfo] = [
    CompanyInfo("I-ne",         "4933", "グロース", "Domestic EC", "jquants"),
    CompanyInfo("北の達人",      "2930", "プライム", "Domestic EC", "jquants"),
    CompanyInfo("Oisix",        "3182", "プライム", "Domestic EC", "jquants"),
    CompanyInfo("アシックス",    "7936", "プライム", "Sports Apparel", "jquants"),
    CompanyInfo("ゴールドウイン", "8111", "プライム", "Sports Apparel", "jquants"),
    CompanyInfo("MTG",          "7806", "プライム", "Wellness", "jquants"),
    CompanyInfo("ヤーマン",      "6630", "プライム", "Wellness", "jquants"),
    CompanyInfo("On Holdings",  "ONON", "NYSE",    "Global D2C", "yfinance", "ONON"),
    CompanyInfo("lululemon",    "LULU", "NASDAQ",  "Global D2C", "yfinance", "LULU"),
    CompanyInfo("FIGS",         "FIGS", "NYSE",    "Global D2C", "yfinance", "FIGS"),
    CompanyInfo("YETI",         "YETI", "NYSE",    "Global D2C", "yfinance", "YETI"),
    CompanyInfo("Olaplex",      "OLPX", "NASDAQ",  "Global D2C", "yfinance", "OLPX"),
    CompanyInfo("AIロボティクス", "247A", "グロース", "IPO Peers", "jquants"),
    CompanyInfo("yutori",        "5892", "グロース", "IPO Peers", "jquants"),
    CompanyInfo("Human Made",    "456A", "グロース", "IPO Peers", "jquants"),
]

BENCHMARKS: Dict[str, dict] = {
    "TOPIX":      {"source": "jquants", "code": "TOPIX",     "yfinance_ticker": "^TPX"},
    "グロース250": {"source": "jquants", "code": "Growth250", "yfinance_ticker": "2516.T"},
    "日経平均":    {"source": "yfinance", "code": "N225",     "yfinance_ticker": "^N225"},
    "S&P 500":    {"source": "yfinance", "code": "SPX",      "yfinance_ticker": "^GSPC"},
}

JP_STOCK_CODES = [TENTIAL.code] + [c.code for c in COMPS if c.source == "jquants"]
US_STOCK_TICKERS = [c.yfinance_ticker for c in COMPS if c.source == "yfinance"]
CATEGORY_ORDER = ["自社", "Domestic EC", "Sports Apparel", "Wellness", "Global D2C", "IPO Peers"]
REPORT_LOOKBACK_WEEKS = 12
VOLUME_LOOKBACK_DAYS = 20
SENTIMENT_KEYWORDS = ["TENTIAL", "325A", "テンシャル", "BAKUNE", "バクネ"]
BRAND_COLORS = {
    "navy": "#284B7D", "blue": "#567EB0", "beige": "#E4C9A5",
    "gray": "#858585", "dark": "#595757", "light": "#D8D9D9",
    "red": "#C94444", "green": "#2E8B57",
}
