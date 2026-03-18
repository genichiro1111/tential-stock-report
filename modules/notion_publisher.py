"""
Module 4: Notion Report Publisher (DB対応版)
分析結果をNotion Database の行として追加 + 詳細ページを生成
"""
import datetime, logging
from typing import Dict, List, Optional
from pathlib import Path
import requests
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import NOTION_API_KEY, NOTION_PARENT_PAGE_ID, NOTION_DATABASE_ID, CATEGORY_ORDER, GITHUB_PAGES_BASE_URL
from modules.quantitative import QuantReport, StockPerformance
from modules.qualitative import QualReport

logger = logging.getLogger(__name__)

class NotionClient:
    BASE_URL = "https://api.notion.com/v1"
    VERSION = "2022-06-28"
    def __init__(self, api_key=NOTION_API_KEY):
        self.s = requests.Session()
        self.s.headers.update({"Authorization":f"Bearer {api_key}","Notion-Version":self.VERSION,"Content-Type":"application/json"})

    def _req(self, method, ep, json_data=None, timeout=60):
        try:
            r = self.s.request(method, f"{self.BASE_URL}{ep}", json=json_data, timeout=timeout)
            r.raise_for_status(); return r.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Notion [{method} {ep}]: {e}")
            if hasattr(e,'response') and e.response: logger.error(e.response.text[:500])
            return {}

    def update_database(self, db_id, properties):
        """DBスキーマにプロパティを追加"""
        return self._req("PATCH", f"/databases/{db_id}", {"properties": properties})

    def create_database(self, parent_page_id, title, properties):
        return self._req("POST","/databases",{"parent":{"type":"page_id","page_id":parent_page_id},"title":[{"type":"text","text":{"content":title}}],"is_inline":True,"properties":properties})

    def query_database(self, db_id, filter_obj=None):
        p = {}
        if filter_obj: p["filter"] = filter_obj
        return self._req("POST",f"/databases/{db_id}/query",p)

    def create_db_page(self, db_id, properties, children=None):
        p = {"parent":{"database_id":db_id},"properties":properties}
        if children: p["children"] = children[:100]
        return self._req("POST","/pages",p)

    def append_blocks(self, page_id, children):
        return self._req("PATCH",f"/blocks/{page_id}/children",{"children":children[:100]})

DB_SCHEMA = {
    "レポート名":{"title":{}},
    "対象週":{"date":{}},
    "TENTIAL終値":{"number":{"format":"yen"}},
    "TENTIAL週間騰落率":{"number":{"format":"percent"}},
    "グロース250週間":{"number":{"format":"percent"}},
    "vs グロース250":{"number":{"format":"percent"}},
    "カテゴリ":{"select":{"options":[{"name":"定期レポート","color":"blue"},{"name":"臨時レポート","color":"red"}]}},
    "ステータス":{"select":{"options":[{"name":"配信済み","color":"green"},{"name":"下書き","color":"yellow"},{"name":"エラー","color":"red"}]}},
    "貸借倍率":{"number":{"format":"number"}},
    "レポートURL":{"url":{}},
}

class B:
    """Block builders"""
    @staticmethod
    def h2(t,e=""): return {"object":"block","type":"heading_2","heading_2":{"rich_text":[{"type":"text","text":{"content":f"{e} {t}" if e else t}}]}}
    @staticmethod
    def h3(t): return {"object":"block","type":"heading_3","heading_3":{"rich_text":[{"type":"text","text":{"content":t}}]}}
    @staticmethod
    def p(t,bold=False,color="default"): return {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":t},"annotations":{"bold":bold,"color":color}}]}}
    @staticmethod
    def callout(t,emoji="💡",color="gray_background"): return {"object":"block","type":"callout","callout":{"icon":{"type":"emoji","emoji":emoji},"color":color,"rich_text":[{"type":"text","text":{"content":t}}]}}
    @staticmethod
    def divider(): return {"object":"block","type":"divider","divider":{}}
    @staticmethod
    def table(rows,header=True):
        if not rows: return B.p("（データなし）")
        w = len(rows[0])
        trs = [{"object":"block","type":"table_row","table_row":{"cells":[[{"type":"text","text":{"content":str(c)}}] for c in r]}} for r in rows]
        return {"object":"block","type":"table","table":{"table_width":w,"has_column_header":header,"has_row_header":False,"children":trs}}
    @staticmethod
    def bullet(t): return {"object":"block","type":"bulleted_list_item","bulleted_list_item":{"rich_text":[{"type":"text","text":{"content":t}}]}}
    @staticmethod
    def toggle(title,children): return {"object":"block","type":"toggle","toggle":{"rich_text":[{"type":"text","text":{"content":title}}],"children":children}}
    @staticmethod
    def quote(t): return {"object":"block","type":"quote","quote":{"rich_text":[{"type":"text","text":{"content":t}}]}}

def _fp(v):
    s = "+" if v > 0 else ""
    return f"{s}{v:.1f}%"

class NotionReportComposer:
    def __init__(self, quant: QuantReport, qual: QualReport):
        self.q = quant; self.ql = qual

    def compose_blocks(self):
        t = self.q.tential; blocks = []
        # Executive Summary
        blocks.append(B.h2("Executive Summary","🎯"))
        g = self.q.benchmarks.get("グロース250")
        vs = ""
        if g:
            d = t.weekly_return - g.weekly_return
            vs = f"グロース250（{_fp(g.weekly_return)}）を{'アウトパフォーム' if d>0 else 'アンダーパフォーム'}。"
        blocks.append(B.callout(f"TENTIAL（325A）は前週比{_fp(t.weekly_return)}。{vs}","💡","blue_background"))
        blocks.append(B.divider())
        # Market Overview
        blocks.append(B.h2("マーケット概況","🌏"))
        rows = [["指数","終値","週間","月初来","年初来"]]
        for n in ["日経平均","TOPIX","グロース250","S&P 500"]:
            bm = self.q.benchmarks.get(n)
            if bm: rows.append([n,f"{bm.close:,.0f}" if bm.close>100 else f"{bm.close:,.2f}",_fp(bm.weekly_return),_fp(bm.mtd_return),_fp(bm.ytd_return)])
        blocks.append(B.table(rows))
        ms = self.ql.market_sentiment
        if ms.jp_market_summary:
            blocks.append(B.callout(f"📝 市況メモ\n{ms.jp_market_summary}\n\n{ms.growth_market_summary}","📝"))
        blocks.append(B.divider())
        # TENTIAL
        blocks.append(B.h2("TENTIAL（325A）パフォーマンス","📈"))
        rng = f"¥{t.weekly_low:,.0f}—¥{t.weekly_high:,.0f}" if t.weekly_high else "N/A"
        tv = f"¥{t.avg_turnover/1e6:,.0f}M" if t.avg_turnover else "N/A"
        blocks.append(B.callout(f"終値: ¥{t.close:,.0f}（前週比 {_fp(t.weekly_return)}）\n週間レンジ: {rng}\n売買代金: {tv}（前週比 {_fp(t.turnover_change)}）\n月初来: {_fp(t.mtd_return)} / 年初来: {_fp(t.ytd_return)}","📊","blue_background"))
        blocks.append(B.divider())
        # Comps
        blocks.append(B.h2("Comps パフォーマンス比較","🔥"))
        blocks.append(B.h3("週間騰落率ランキング"))
        all_s = sorted([t]+self.q.comps, key=lambda x:x.weekly_return, reverse=True)
        for i,s in enumerate(all_s,1):
            mk = " ⭐" if s.code=="325A" else ""
            em = "🟢" if s.weekly_return>1 else ("🔴" if s.weekly_return<-1 else "⚪")
            blocks.append(B.bullet(f"{em} {i}位: {s.name}（{s.code}）{_fp(s.weekly_return)}{mk}"))
        blocks.append(B.h3("カテゴリ別 詳細テーブル"))
        rows = [["カテゴリ","企業名","コード","終値","週間","月初来","年初来"]]
        rows.append(["⭐ 自社",t.name,t.code,f"¥{t.close:,.0f}",_fp(t.weekly_return),_fp(t.mtd_return),_fp(t.ytd_return)])
        for cat in CATEGORY_ORDER:
            if cat=="自社": continue
            cs = [c for c in self.q.comps if c.category==cat]
            for j,c in enumerate(cs):
                cl = cat if j==0 else ""
                pr = f"¥{c.close:,.0f}" if c.close>100 else f"${c.close:,.2f}"
                rows.append([cl,c.name,c.code,pr,_fp(c.weekly_return),_fp(c.mtd_return),_fp(c.ytd_return)])
        blocks.append(B.table(rows))
        blocks.append(B.h3("カテゴリ別 平均騰落率"))
        cr = [["カテゴリ","週間平均","月初来","年初来","Best","Worst"]]
        for cp in self.q.categories:
            cr.append([cp.category,_fp(cp.avg_weekly_return),_fp(cp.avg_mtd_return),_fp(cp.avg_ytd_return),cp.best_performer,cp.worst_performer])
        blocks.append(B.table(cr))
        blocks.append(B.divider())
        # Supply/Demand
        blocks.append(B.h2("需給分析","⚖️"))
        m = self.q.margin
        if m.long_balance or m.short_balance:
            blocks.append(B.table([["項目","残高（株）","前週比"],["信用買い残",f"{m.long_balance:,}",_fp(m.long_change_pct)],["信用売り残",f"{m.short_balance:,}",_fp(m.short_change_pct)],["貸借倍率",f"{m.ratio:.1f}x","—"]]))
        else:
            blocks.append(B.callout("信用残データは取得後に自動表示されます","📋"))
        blocks.append(B.divider())
        # Qualitative
        blocks.append(B.h2("定性分析（モメンタム）","💬"))
        yb = self.ql.yahoo_bbs
        if yb.post_count > 0:
            blocks.append(B.callout(
                f"Yahoo掲示板センチメント: {yb.trend}（投稿{yb.post_count}件）\n"
                f"強気 {yb.bullish_pct:.0f}% / 中立 {yb.neutral_pct:.0f}% / 弱気 {yb.bearish_pct:.0f}%",
                "🗣️", "blue_background"
            ))
            # Topic categories
            if yb.topic_categories:
                blocks.append(B.h3("トピック分析"))
                topic_rows = [["トピック", "件数", "割合"]]
                for tc in yb.topic_categories:
                    topic_rows.append([tc.name, str(tc.count), f"{tc.pct:.0f}%"])
                blocks.append(B.table(topic_rows))
            # Notable comments
            if yb.notable_comments:
                blocks.append(B.h3("注目投稿"))
                for c in yb.notable_comments[:5]:
                    blocks.append(B.quote(c))
        else:
            blocks.append(B.callout("定性分析はスケジュールタスク実行時にWebSearchで自動収集されます。\n（Yahoo掲示板センチメント・X言及・市況感サマリ）","📋"))
        blocks.append(B.divider())
        # Watchpoints
        blocks.append(B.h2("来週のウォッチポイント","👁️"))
        for ev in self.ql.market_sentiment.key_events_next_week:
            blocks.append(B.bullet(ev))
        # Footer
        blocks.append(B.divider())
        blocks.append(B.p(f"📊 自動生成（{datetime.datetime.now():%Y-%m-%d %H:%M}）| TENTIAL IR",color="gray"))
        return blocks

    def compose_db_properties(self, week_start, week_end, report_url=None):
        t = self.q.tential; wn = datetime.date.today().isocalendar()[1]
        g = self.q.benchmarks.get("グロース250")
        gr = g.weekly_return if g else 0.0
        props = {
            "レポート名":{"title":[{"text":{"content":f"W{wn} — {week_start} 〜 {week_end}"}}]},
            "対象週":{"date":{"start":week_start,"end":week_end}},
            "TENTIAL終値":{"number":t.close if t.close else None},
            "TENTIAL週間騰落率":{"number":round(t.weekly_return/100,4) if t.weekly_return else None},
            "グロース250週間":{"number":round(gr/100,4) if gr else None},
            "vs グロース250":{"number":round((t.weekly_return-gr)/100,4) if t.weekly_return else None},
            "カテゴリ":{"select":{"name":"定期レポート"}},
            "ステータス":{"select":{"name":"配信済み"}},
            "貸借倍率":{"number":round(self.q.margin.ratio,2) if self.q.margin.ratio else None},
        }
        if report_url:
            props["レポートURL"] = {"url": report_url}
        return props

class NotionPublisher:
    def __init__(self):
        self.client = NotionClient()
        self.db_id = NOTION_DATABASE_ID

    def _ensure_database(self):
        if self.db_id:
            logger.info(f"Using DB: {self.db_id}")
            # 既存DBにレポートURLプロパティがなければ追加
            self.client.update_database(self.db_id, {"レポートURL": {"url": {}}})
            return self.db_id
        logger.info("Creating new database...")
        r = self.client.create_database(NOTION_PARENT_PAGE_ID, "📊 Weekly Stock Reports", DB_SCHEMA)
        if r.get("id"):
            self.db_id = r["id"]
            logger.info(f"✅ DB created: {self.db_id}")
            # Save to .env
            env_path = Path(__file__).resolve().parent.parent / ".env"
            if env_path.exists():
                content = env_path.read_text()
                if "NOTION_DATABASE_ID=" in content:
                    lines = content.split("\n")
                    lines = [f"NOTION_DATABASE_ID={self.db_id}" if l.startswith("NOTION_DATABASE_ID=") else l for l in lines]
                    env_path.write_text("\n".join(lines))
                else:
                    with open(env_path,"a") as f: f.write(f"\nNOTION_DATABASE_ID={self.db_id}\n")
                logger.info("💾 DB ID saved to .env")
            return self.db_id
        return ""

    def _find_existing(self, week_start):
        """同じ対象週のレポートがあれば既存ページIDを返す"""
        if not self.db_id: return None
        r = self.client.query_database(self.db_id, {"property":"対象週","date":{"equals":week_start}})
        results = r.get("results", [])
        if results:
            return results[0].get("id")
        return None

    def _archive_page(self, page_id):
        """既存ページをアーカイブ（削除）"""
        r = self.client._req("PATCH", f"/pages/{page_id}", {"archived": True})
        if r:
            logger.info(f"🗑️ Archived existing page: {page_id}")
        return r

    def publish(self, quant, qual, metadata=None, pages_path=None):
        db_id = self._ensure_database()
        if not db_id: return None
        ws = metadata.get("week_start","") if metadata else ""
        we = metadata.get("week_end","") if metadata else ""
        if not ws:
            today = datetime.date.today()
            dsf = (today.weekday()-4)%7; ed = today-datetime.timedelta(days=dsf)
            ws, we = (ed-datetime.timedelta(days=4)).isoformat(), ed.isoformat()
        # 既存レポートがあればアーカイブして再作成
        existing_id = self._find_existing(ws)
        if existing_id:
            logger.info(f"♻️ Report for {ws} exists — replacing...")
            self._archive_page(existing_id)
        # Build GitHub Pages URL
        report_url = None
        if GITHUB_PAGES_BASE_URL and pages_path:
            base = GITHUB_PAGES_BASE_URL.rstrip("/")
            report_url = f"{base}/{pages_path}"
        composer = NotionReportComposer(quant, qual)
        props = composer.compose_db_properties(ws, we, report_url=report_url)
        blocks = composer.compose_blocks()
        r = self.client.create_db_page(db_id, props, blocks[:100])
        if not r or "id" not in r: return None
        if len(blocks) > 100:
            for i in range(100, len(blocks), 100):
                self.client.append_blocks(r["id"], blocks[i:i+100])
        url = r.get("url","")
        logger.info(f"📄 Published: {url}")
        return url
