"""
Module 3: Qualitative / Sentiment Analysis
Yahoo Finance 掲示板スクレイピング + Google News RSS（直近3日フィルター付き）
追加の依存ライブラリ不要（requests + xml.etree + re のみ）
"""
import logging, os, re, datetime
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from urllib.parse import quote
from email.utils import parsedate_to_datetime
import requests
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SENTIMENT_KEYWORDS

logger = logging.getLogger(__name__)

# ── Data Classes ──────────────────────────────────

@dataclass
class NewsItem:
    title: str = ""
    source: str = ""
    date: str = ""
    url: str = ""
    relevance: str = ""  # "tential", "growth", "market", "sector"

@dataclass
class ForumPost:
    """Yahoo Finance 掲示板の個別投稿"""
    user: str = ""
    post_no: str = ""
    date: str = ""            # "2026/3/17 15:26"
    body: str = ""
    yes_count: int = 0        # 「はい」の数
    no_count: int = 0         # 「いいえ」の数
    engagement: int = 0       # yes + no

@dataclass
class SentimentResult:
    source: str; post_count: int = 0; post_count_prev: int = 0
    sentiment_score: float = 0.0; bullish_pct: float = 0.0
    neutral_pct: float = 0.0; bearish_pct: float = 0.0
    trend: str = "横ばい"; notable_comments: List[str] = field(default_factory=list)
    top_topics: List[str] = field(default_factory=list)
    forum_posts: List[ForumPost] = field(default_factory=list)

@dataclass
class MarketSentiment:
    jp_market_summary: str = ""; growth_market_summary: str = ""
    global_summary: str = ""; key_events_next_week: List[str] = field(default_factory=list)
    market_news: List[NewsItem] = field(default_factory=list)
    tential_news: List[NewsItem] = field(default_factory=list)
    sector_news: List[NewsItem] = field(default_factory=list)

@dataclass
class QualReport:
    yahoo_bbs: SentimentResult = field(default_factory=lambda: SentimentResult(source="yahoo_bbs"))
    twitter: SentimentResult = field(default_factory=lambda: SentimentResult(source="twitter"))
    market_sentiment: MarketSentiment = field(default_factory=MarketSentiment)


# ── Shared ────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

BULLISH_WORDS = [
    "上昇", "急伸", "高値", "買い", "反発", "好調", "増益", "増収",
    "成長", "拡大", "堅調", "プラス", "最高", "回復", "強気",
    "買い増し", "仕込", "期待", "底打ち", "ナンピン", "ホールド",
]
BEARISH_WORDS = [
    "下落", "急落", "安値", "売り", "続落", "低迷", "減益", "減収",
    "縮小", "悪化", "マイナス", "懸念", "リスク", "弱気", "暴落",
    "損切", "ヤバい", "クソ", "終わり", "だめ", "ダメ", "売り煽り",
]


def _classify_sentiment(texts: List[str]) -> Dict[str, float]:
    """テキストリストからセンチメント分類（キーワードベース）"""
    b, n, br = 0, 0, 0
    for t in texts:
        is_bull = any(w in t for w in BULLISH_WORDS)
        is_bear = any(w in t for w in BEARISH_WORDS)
        if is_bull and not is_bear: b += 1
        elif is_bear and not is_bull: br += 1
        else: n += 1
    total = max(b + n + br, 1)
    return {
        "bullish": round(b / total * 100),
        "neutral": round(n / total * 100),
        "bearish": round(br / total * 100),
    }


def _is_within_days(date_str: str, days: int = 7) -> bool:
    """RSS日付文字列が直近N日以内かチェック (RFC 2822形式)"""
    try:
        dt = parsedate_to_datetime(date_str)
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        return dt >= cutoff
    except Exception:
        return True  # パース失敗時は含める（フレッシュと仮定）


# ── Yahoo Finance 掲示板スクレイパー ──────────────

FORUM_URL = "https://finance.yahoo.co.jp/quote/325A.T/forum"
# 投稿パターン: ユーザー名 + No.XXXXX + 日時 + 報告 + 本文 + はいN + いいえN
_POST_RE = re.compile(
    r'(.+?)(No\.\d+)(\d{4}/\d+/\d+\s+\d+:\d+)報告(.+?)返信投資の参考になりましたか？はい(\d+)いいえ(\d+)',
    re.DOTALL
)


def _fetch_yahoo_forum(max_posts: int = 50, days: int = 7) -> List[ForumPost]:
    """Yahoo Finance 掲示板 (325A.T) をスクレイピングし、直近N日の投稿を返す"""
    try:
        resp = requests.get(FORUM_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        html = resp.text

        # <article> タグ内のテキストを抽出
        # BeautifulSoup 不要: 正規表現で <article>...</article> を抽出
        article_re = re.compile(r'<article[^>]*>(.*?)</article>', re.DOTALL)
        articles = article_re.findall(html)
        logger.info(f"  Yahoo掲示板: {len(articles)} article要素を検出")

        cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
        posts = []

        for art_html in articles:
            # HTMLタグ除去
            text = re.sub(r'<[^>]+>', '', art_html).strip()
            text = re.sub(r'\s+', ' ', text)

            m = _POST_RE.match(text)
            if not m:
                continue

            user = m.group(1).strip()
            post_no = m.group(2)
            date_str = m.group(3).strip()
            body = m.group(4).strip()
            yes_count = int(m.group(5))
            no_count = int(m.group(6))

            # 日付フィルター
            try:
                dt = datetime.datetime.strptime(date_str, "%Y/%m/%d %H:%M")
                if dt < cutoff:
                    continue
            except ValueError:
                pass  # パース失敗は含める

            posts.append(ForumPost(
                user=user,
                post_no=post_no,
                date=date_str,
                body=body[:200],  # 長すぎる投稿は切る
                yes_count=yes_count,
                no_count=no_count,
                engagement=yes_count + no_count,
            ))

            if len(posts) >= max_posts:
                break

        return posts

    except Exception as e:
        logger.warning(f"Yahoo Finance forum scraping error: {e}")
        return []


def _analyze_forum_posts(posts: List[ForumPost]) -> Dict:
    """掲示板投稿リストからセンチメント・注目投稿を分析"""
    if not posts:
        return {"bullish": 0, "neutral": 100, "bearish": 0, "notable": [], "trend": "データなし"}

    bodies = [p.body for p in posts]
    sent = _classify_sentiment(bodies)

    # 注目投稿: engagement (はい+いいえ) が多い順にソート
    sorted_by_engagement = sorted(posts, key=lambda p: p.engagement, reverse=True)
    notable = []
    for p in sorted_by_engagement[:5]:
        # 短すぎる投稿は除外
        if len(p.body) >= 5:
            notable.append(f"[👍{p.yes_count} 👎{p.no_count}] {p.body}")

    # トレンド判定
    if sent["bullish"] > sent["bearish"] + 10:
        trend = "強気"
    elif sent["bearish"] > sent["bullish"] + 10:
        trend = "弱気"
    elif sent["bullish"] > sent["bearish"]:
        trend = "やや強気"
    elif sent["bearish"] > sent["bullish"]:
        trend = "やや弱気"
    else:
        trend = "中立"

    return {**sent, "notable": notable, "trend": trend}


# ── Google News RSS（1週間フィルター付き） ────────

def _fetch_google_news_rss(query: str, max_items: int = 5, days: int = 7) -> List[NewsItem]:
    """Google News RSS で検索し、直近N日のニュースのみ返す"""
    # Google News RSS の when パラメータで期間指定
    url = f"https://news.google.com/rss/search?q={quote(query)}+when:{days}d&hl=ja&gl=JP&ceid=JP:ja"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall(".//item"):
            if len(items) >= max_items:
                break
            title = item.findtext("title", "")
            source = item.findtext("source", "")
            pub_date = item.findtext("pubDate", "")
            link = item.findtext("link", "")

            # 追加の日付フィルター（RSSの when パラメータが不完全な場合のバックアップ）
            if pub_date and not _is_within_days(pub_date, days):
                continue

            # Clean title (remove source suffix like " - 日経新聞")
            clean_title = re.sub(r'\s*-\s*[^-]+$', '', title).strip()
            if not clean_title:
                continue

            items.append(NewsItem(
                title=clean_title,
                source=source,
                date=pub_date,
                url=link,
            ))
        return items
    except Exception as e:
        logger.warning(f"Google News RSS error for '{query}': {e}")
        return []


# ── Analyzer ──────────────────────────────────────

class QualitativeAnalyzer:
    def analyze(self) -> QualReport:
        report = QualReport()
        logger.info("📰 Fetching qualitative data...")

        # ── 1. Yahoo Finance 掲示板（TENTIAL 325A） ──
        logger.info("  🗣️ Yahoo Finance 掲示板をスクレイピング中...")
        forum_posts = _fetch_yahoo_forum(max_posts=50, days=3)
        logger.info(f"  掲示板: 直近3日の投稿 {len(forum_posts)} 件")

        forum_analysis = _analyze_forum_posts(forum_posts)
        report.yahoo_bbs.post_count = len(forum_posts)
        report.yahoo_bbs.bullish_pct = forum_analysis["bullish"]
        report.yahoo_bbs.neutral_pct = forum_analysis["neutral"]
        report.yahoo_bbs.bearish_pct = forum_analysis["bearish"]
        report.yahoo_bbs.trend = forum_analysis["trend"]
        report.yahoo_bbs.notable_comments = forum_analysis["notable"][:5]
        report.yahoo_bbs.forum_posts = forum_posts

        # ── 2. TENTIAL / BAKUNE 関連ニュース（Google News, 直近3日） ──
        tential_news = []
        for kw in ["TENTIAL 株", "BAKUNE", "テンシャル 325A"]:
            tential_news.extend(_fetch_google_news_rss(kw, 3, days=3))
        # Deduplicate by title
        seen = set()
        unique_tential = []
        for n in tential_news:
            if n.title not in seen:
                seen.add(n.title)
                n.relevance = "tential"
                unique_tential.append(n)
        report.market_sentiment.tential_news = unique_tential[:5]
        logger.info(f"  TENTIAL関連ニュース: {len(unique_tential)} 件")

        # ── 3. マーケット全体ニュース（日経平均・TOPIX, 直近3日） ──
        market_news = _fetch_google_news_rss("日経平均 株式市場 今週", 5, days=3)
        for n in market_news:
            n.relevance = "market"
        report.market_sentiment.market_news = market_news
        logger.info(f"  市場全体: {len(market_news)} 件")

        # マーケットサマリを生成
        if market_news:
            titles = [n.title for n in market_news]
            sent = _classify_sentiment(titles)
            tone = "強気" if sent["bullish"] > sent["bearish"] else ("弱気" if sent["bearish"] > sent["bullish"] else "中立")
            report.market_sentiment.jp_market_summary = (
                f"日本株市場: {tone}ムード。主要ニュース — {titles[0]}"
            )

        # ── 4. グロース市場・小型株ニュース（直近3日） ──
        growth_news = _fetch_google_news_rss("東証グロース市場 個人投資家 小型株", 4, days=3)
        for n in growth_news:
            n.relevance = "growth"
        if growth_news:
            report.market_sentiment.growth_market_summary = (
                f"グロース市場: {growth_news[0].title}"
            )

        # ── 5. セクターニュース（D2C・スポーツ・ウェルネス, 直近3日） ──
        sector_queries = [
            "D2C EC 消費 株",
            "スポーツアパレル アシックス 株",
            "ウェルネス 健康 リカバリー 株",
        ]
        sector_news = []
        for sq in sector_queries:
            items = _fetch_google_news_rss(sq, 2, days=3)
            for n in items:
                n.relevance = "sector"
            sector_news.extend(items)
        report.market_sentiment.sector_news = sector_news[:6]
        logger.info(f"  セクター: {len(sector_news)} 件")

        # ── 6. 来週のイベント（直近3日のニュースから） ──
        next_monday = datetime.date.today() + datetime.timedelta(days=(7 - datetime.date.today().weekday()) % 7)
        event_news = _fetch_google_news_rss(f"来週 経済指標 日銀 FOMC {next_monday.month}月", 5, days=3)
        events = [n.title for n in event_news if len(n.title) > 5]
        report.market_sentiment.key_events_next_week = events[:5]
        logger.info(f"  来週イベント: {len(events)} 件")

        # ── 7. グローバルサマリ（直近3日） ──
        global_news = _fetch_google_news_rss("米国株 S&P500 今週", 3, days=3)
        if global_news:
            report.market_sentiment.global_summary = f"米国市場: {global_news[0].title}"

        logger.info("✅ Qualitative analysis complete")
        return report
