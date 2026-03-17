"""
Module 7: Slack Publisher
サマリーテキスト + GitHub Pages URLをSlackチャンネルに投稿
"""
import logging
from typing import Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SLACK_BOT_TOKEN, SLACK_CHANNEL_ID, GITHUB_PAGES_BASE_URL

logger = logging.getLogger(__name__)


class SlackPublisher:
    def __init__(self, token: str = SLACK_BOT_TOKEN, channel: str = SLACK_CHANNEL_ID):
        self.client = WebClient(token=token)
        self.channel = channel

    def post_report(
        self,
        summary_text: str,
        report_url: Optional[str] = None,
        notion_url: Optional[str] = None,
    ) -> Optional[str]:
        """サマリー + レポートURLをSlackに投稿。
        Returns: メッセージのts or None on failure
        """
        if not self.channel:
            logger.error("SLACK_CHANNEL_ID is not set")
            return None

        try:
            # メインメッセージ投稿
            resp = self.client.chat_postMessage(
                channel=self.channel,
                text=summary_text,
                unfurl_links=False,
            )
            ts = resp.get("ts")
            logger.info(f"✅ Slack: summary posted to #{self.channel}")

            # スレッドにリンク集を追加
            links = []
            if report_url:
                links.append(f"🌐 <{report_url}|Web版レポートを見る>")
            if notion_url:
                links.append(f"📝 <{notion_url}|Notionで見る>")
            # アーカイブページ
            if GITHUB_PAGES_BASE_URL:
                links.append(f"📚 <{GITHUB_PAGES_BASE_URL}|過去のレポート一覧>")

            if links and ts:
                self.client.chat_postMessage(
                    channel=self.channel,
                    thread_ts=ts,
                    text="\n".join(links),
                    unfurl_links=False,
                )

            return ts

        except SlackApiError as e:
            logger.error(f"Slack API error: {e.response['error']}")
            return None
        except Exception as e:
            logger.error(f"Slack publish error: {e}")
            return None

    def post_message(self, text: str, thread_ts: Optional[str] = None) -> Optional[str]:
        """テキストメッセージのみ投稿"""
        try:
            resp = self.client.chat_postMessage(
                channel=self.channel,
                text=text,
                thread_ts=thread_ts,
            )
            return resp.get("ts")
        except SlackApiError as e:
            logger.error(f"Slack message error: {e.response['error']}")
            return None


def build_summary_text(quant_report, report_url: Optional[str] = None) -> str:
    """QuantReportからSlack投稿用のサマリーテキストを生成"""
    t = quant_report.tential
    sign = "+" if t.weekly_return > 0 else ""

    lines = [
        f"📊 *TENTIAL Weekly Stock Report*",
        f"",
        f"*TENTIAL（325A）* ¥{t.close:,.0f}  {sign}{t.weekly_return:.1f}%（週間）",
        f"  月初来 {t.mtd_return:+.1f}%  /  年初来 {t.ytd_return:+.1f}%",
    ]

    # ベンチマーク比較
    g = quant_report.benchmarks.get("グロース250")
    if g:
        diff = t.weekly_return - g.weekly_return
        emoji = "🟢" if diff > 0 else "🔴"
        lines.append(f"  {emoji} vs グロース250: {diff:+.1f}%")

    # Comps上位/下位
    if quant_report.comps:
        sorted_comps = sorted(quant_report.comps, key=lambda c: c.weekly_return, reverse=True)
        best = sorted_comps[0]
        worst = sorted_comps[-1]
        lines.append(f"")
        lines.append(f"📈 Best: {best.name} {best.weekly_return:+.1f}%  📉 Worst: {worst.name} {worst.weekly_return:+.1f}%")

    # 信用残
    m = quant_report.margin
    if m.long_balance:
        lines.append(f"⚖️ 信用 買残 {m.long_balance:,} / 売残 {m.short_balance:,} / 倍率 {m.ratio:.2f}x")

    # レポートURL
    if report_url:
        lines.append(f"")
        lines.append(f"👉 <{report_url}|詳細レポートはこちら>")

    return "\n".join(lines)
