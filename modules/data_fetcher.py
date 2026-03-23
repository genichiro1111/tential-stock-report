"""
Module 1: Stock Price Data Fetcher
J-Quants API V2（日本株）+ yfinance（米国株・指数）
PER: カブタン（日本株）+ Google Finance（米国株）+ JSONキャッシュ（フォールバック）
"""
import datetime, json, logging, re, time
from pathlib import Path
from typing import Dict, Optional
import pandas as pd, requests, yfinance as yf
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (JQUANTS_API_KEY, TENTIAL, COMPS, BENCHMARKS,
                              JP_STOCK_CODES, US_STOCK_TICKERS, REPORT_LOOKBACK_WEEKS,
                              EDINETDB_API_KEY)

logger = logging.getLogger(__name__)

# PER キャッシュファイルパス
PER_CACHE_PATH = Path(__file__).resolve().parent.parent / "cache" / "per_cache.json"


class JQuantsClient:
    """J-Quants API V2 client — x-api-key 認証"""
    BASE_URL = "https://api.jquants.com/v2"

    def __init__(self, api_key: str = JQUANTS_API_KEY):
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": api_key})

    def _get(self, endpoint, params=None):
        try:
            resp = self.session.get(f"{self.BASE_URL}{endpoint}", params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"J-Quants API error: {endpoint} → {e}")
            return {}

    def get_daily_quotes(self, code, from_date, to_date):
        """日足OHLCV取得 — V2レスポンス形式対応"""
        # V2: code は5桁（例: 325A0）
        code_v2 = code if len(code) >= 5 else code + "0"
        params = {"code": code_v2, "from": from_date.replace("-", ""), "to": to_date.replace("-", "")}
        data = self._get("/equities/bars/daily", params)
        if not data or "data" not in data:
            return pd.DataFrame()
        df = pd.DataFrame(data["data"])
        # V2 pagination
        while "pagination_key" in data:
            params["pagination_key"] = data["pagination_key"]
            data = self._get("/equities/bars/daily", params)
            if data and "data" in data:
                df = pd.concat([df, pd.DataFrame(data["data"])], ignore_index=True)
        if df.empty:
            return df
        # V2 columns: Date, Code, O, H, L, C, Vo, Va, AdjO, AdjH, AdjL, AdjC, AdjVo, AdjFactor
        rename = {
            "Date": "Date", "Code": "Code",
            "O": "Open", "H": "High", "L": "Low", "C": "Close",
            "AdjO": "AdjOpen", "AdjH": "AdjHigh", "AdjL": "AdjLow", "AdjC": "AdjClose",
            "Vo": "Volume", "Va": "TurnoverValue", "AdjVo": "AdjVolume",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date").reset_index(drop=True)
        return df

    def get_topix(self, from_date, to_date):
        """TOPIX指数 — V2 専用エンドポイント"""
        params = {"from": from_date.replace("-", ""), "to": to_date.replace("-", "")}
        data = self._get("/indices/bars/daily/topix", params)
        if not data or "data" not in data:
            return pd.DataFrame()
        df = pd.DataFrame(data["data"])
        # V2 columns: Date, O, H, L, C
        rename = {"O": "Open", "H": "High", "L": "Low", "C": "Close"}
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date").reset_index(drop=True)
        return df

    def get_indices(self, index_code, from_date, to_date):
        """一般指数データ取得 — V2"""
        params = {"code": index_code, "from": from_date.replace("-", ""), "to": to_date.replace("-", "")}
        data = self._get("/indices/bars/daily", params)
        if not data or "data" not in data:
            return pd.DataFrame()
        df = pd.DataFrame(data["data"])
        rename = {"O": "Open", "H": "High", "L": "Low", "C": "Close"}
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date").reset_index(drop=True)
        return df

    def get_margin_trading(self, code, from_date, to_date):
        """信用残データ取得 — V2: /markets/margin-interest
        V2 columns: Date, Code, LongVol(買い残), ShrtVol(売り残),
                     LongNegVol, ShrtNegVol, LongStdVol, ShrtStdVol, IssType
        """
        code_v2 = code if len(code) >= 5 else code + "0"
        params = {"code": code_v2, "from": from_date.replace("-", ""), "to": to_date.replace("-", "")}
        data = self._get("/markets/margin-interest", params)
        if not data or "data" not in data:
            return pd.DataFrame()
        df = pd.DataFrame(data["data"])
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date").reset_index(drop=True)
        return df


class KabutanScraper:
    """カブタンからPER・PBR等のバリュエーション指標をスクレイピング（日本株専用）"""
    BASE_URL = "https://kabutan.jp/stock/?code={code}"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }

    @staticmethod
    def get_per(code: str) -> float:
        """カブタンから PER を取得。取得失敗時は 0.0 を返す。
        HTML構造: <table> の TH=PER → 同行の TD に「13.2倍」形式で格納"""
        url = KabutanScraper.BASE_URL.format(code=code)
        try:
            resp = requests.get(url, headers=KabutanScraper.HEADERS, timeout=15)
            resp.raise_for_status()
            html = resp.text
            # PER の値を抽出: "PER" の後に出現する "XX.X倍" パターン
            # カブタンのHTML: <th>PER</th> ... <td>13.2倍</td> (同テーブル行内)
            m = re.search(r'PER[^<]*</th>\s*(?:</tr>\s*<tr[^>]*>)?\s*<td[^>]*>\s*([\d,]+\.?\d*)\s*倍', html, re.DOTALL)
            if not m:
                # フォールバック: テーブル外でも "PER" 近くの数値を探す
                m = re.search(r'PER[（(]倍[）)]?\s*</th>\s*<td[^>]*>\s*([\d,]+\.?\d*)', html, re.DOTALL)
            if not m:
                # さらに緩い検索: PER の近くの数字+倍
                m = re.search(r'PER.*?([\d,]+\.\d+)\s*倍', html[:5000], re.DOTALL)
            if m:
                val = m.group(1).replace(",", "")
                return round(float(val), 1)
            logger.warning(f"Kabutan PER not found for {code}")
            return 0.0
        except Exception as e:
            logger.warning(f"Kabutan scraping error for {code}: {e}")
            return 0.0

    @staticmethod
    def get_valuation(code: str) -> Dict[str, float]:
        """カブタンから PER, PBR, 利回り, 信用倍率をまとめて取得
        テーブル構造: <tr><th>PER</th><th>PBR</th>...</tr><tr><td>13.2倍</td><td>2.15倍</td>...</tr>
        """
        url = KabutanScraper.BASE_URL.format(code=code)
        result = {"per": 0.0, "pbr": 0.0, "dividend_yield": 0.0, "margin_ratio": 0.0}
        try:
            resp = requests.get(url, headers=KabutanScraper.HEADERS, timeout=15)
            resp.raise_for_status()
            html = resp.text
            # テーブル行をパースしてヘッダーと値のマッピングを作成
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
            header_row_idx = None
            headers = []
            for i, row in enumerate(rows):
                ths = re.findall(r'<th[^>]*>(.*?)</th>', row, re.DOTALL)
                if any('PER' in th for th in ths):
                    header_row_idx = i
                    headers = [re.sub(r'<[^>]+>', '', th).strip() for th in ths]
                    break
            if header_row_idx is not None and header_row_idx + 1 < len(rows):
                value_row = rows[header_row_idx + 1]
                tds = re.findall(r'<td[^>]*>(.*?)</td>', value_row, re.DOTALL)
                values = [re.sub(r'<[^>]+>', '', td).strip() for td in tds]
                for h, v in zip(headers, values):
                    num_match = re.search(r'([\d,]+\.?\d*)', v)
                    if not num_match:
                        continue
                    num = float(num_match.group(1).replace(",", ""))
                    if 'PER' in h:
                        result["per"] = round(num, 2)
                    elif 'PBR' in h:
                        result["pbr"] = round(num, 2)
                    elif '利回り' in h:
                        result["dividend_yield"] = round(num, 2)
                    elif '信用倍率' in h:
                        result["margin_ratio"] = round(num, 2)
            return result
        except Exception as e:
            logger.warning(f"Kabutan valuation error for {code}: {e}")
            return result


    @staticmethod
    def get_latest_price(code: str) -> Optional[Dict]:
        """カブタンから最新のOHLCV + 日付を取得。
        テーブル構造:
          table[3]: <th>始値</th><td>3,720</td>... / <th>高値</th>... / <th>安値</th>... / <th>終値</th>...
          table[4]: <th>出来高</th><td>62,400 株</td>
        ページ上部に日付: YYYY/MM/DD
        """
        url = KabutanScraper.BASE_URL.format(code=code)
        try:
            resp = requests.get(url, headers=KabutanScraper.HEADERS, timeout=15)
            resp.raise_for_status()
            html = resp.text

            # 日付抽出（ページ内の最初の YYYY/MM/DD）
            date_m = re.search(r'(\d{4}/\d{1,2}/\d{1,2})', html)
            if not date_m:
                return None
            date_str = date_m.group(1)

            # OHLC抽出: <th>始値|高値|安値|終値</th> の同行 <td> から数値取得
            ohlc = {}
            for label, key in [("始値", "Open"), ("高値", "High"), ("安値", "Low"), ("終値", "Close")]:
                m = re.search(
                    rf'<th[^>]*>{label}</th>\s*<td[^>]*>\s*([\d,.]+)',
                    html, re.DOTALL
                )
                if m:
                    ohlc[key] = float(m.group(1).replace(",", ""))

            # 出来高
            vol_m = re.search(r'<th[^>]*>出来高</th>\s*<td[^>]*>\s*([\d,]+)', html, re.DOTALL)
            volume = int(vol_m.group(1).replace(",", "")) if vol_m else 0

            if not ohlc.get("Close"):
                return None

            return {
                "Date": date_str.replace("/", "-"),
                "Open": ohlc.get("Open", ohlc["Close"]),
                "High": ohlc.get("High", ohlc["Close"]),
                "Low": ohlc.get("Low", ohlc["Close"]),
                "Close": ohlc["Close"],
                "Volume": volume,
            }
        except Exception as e:
            logger.warning(f"Kabutan latest price error for {code}: {e}")
            return None


class EdinetDBClient:
    """EDINET DB REST API クライアント（日本株のバリュエーション指標取得）
    https://edinetdb.jp/docs/api
    - /v1/search?q={query} — 認証不要、証券コードで検索→EDINETコード取得
    - /v1/companies/{edinet_code} — APIキー必要、PER/PBR/ROE等を含む企業詳細
    """
    BASE_URL = "https://edinetdb.jp/v1"

    def __init__(self, api_key: str = EDINETDB_API_KEY):
        self.api_key = api_key
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"X-API-Key": api_key})
        # 証券コード→EDINETコードのキャッシュ
        self._code_map: Dict[str, str] = {}

    def _search(self, query: str) -> Optional[str]:
        """証券コードからEDINETコードを検索（認証不要）"""
        try:
            resp = self.session.get(f"{self.BASE_URL}/search", params={"q": query}, timeout=10)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if data:
                edinet_code = data[0].get("edinet_code")
                if edinet_code:
                    self._code_map[query] = edinet_code
                return edinet_code
        except Exception as e:
            logger.warning(f"EDINET DB search error for {query}: {e}")
        return None

    def get_per(self, stock_code: str) -> float:
        """証券コードからPERを取得。APIキーが無い場合は0.0を返す。"""
        if not self.api_key:
            return 0.0
        # 証券コード→EDINETコード
        edinet_code = self._code_map.get(stock_code) or self._search(stock_code)
        if not edinet_code:
            return 0.0
        try:
            resp = self.session.get(f"{self.BASE_URL}/companies/{edinet_code}", timeout=10)
            resp.raise_for_status()
            data = resp.json()
            # 企業詳細レスポンスからPER取得
            per = data.get("per") or data.get("latest_financials", {}).get("per") or 0.0
            return round(float(per), 1) if per else 0.0
        except Exception as e:
            logger.warning(f"EDINET DB company error for {stock_code} ({edinet_code}): {e}")
            return 0.0

    def get_valuation(self, stock_code: str) -> Dict[str, float]:
        """PER, PBR, ROE, 配当利回り等をまとめて取得"""
        result = {"per": 0.0, "pbr": 0.0, "roe": 0.0, "dividend_yield": 0.0}
        if not self.api_key:
            return result
        edinet_code = self._code_map.get(stock_code) or self._search(stock_code)
        if not edinet_code:
            return result
        try:
            resp = self.session.get(f"{self.BASE_URL}/companies/{edinet_code}", timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for key in result:
                val = data.get(key) or data.get("latest_financials", {}).get(key)
                if val:
                    result[key] = round(float(val), 2)
            return result
        except Exception as e:
            logger.warning(f"EDINET DB valuation error for {stock_code}: {e}")
            return result


class YFinanceFetcher:
    @staticmethod
    def get_daily_data(ticker, from_date, to_date):
        try:
            tk = yf.Ticker(ticker)
            df = tk.history(start=from_date, end=to_date)
            if df.empty:
                return pd.DataFrame()
            df = df.reset_index().rename(columns={"index": "Date"})
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
            return df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        except Exception as e:
            logger.error(f"yfinance error for {ticker}: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_per(ticker) -> float:
        """yfinance から PER（株価収益率）を取得"""
        try:
            tk = yf.Ticker(ticker)
            info = tk.info
            # trailingPE or forwardPE
            per = info.get("trailingPE") or info.get("forwardPE") or 0.0
            return round(float(per), 1) if per else 0.0
        except Exception as e:
            logger.warning(f"PER fetch error for {ticker}: {e}")
            return 0.0


class StockDataFetcher:
    def __init__(self):
        self.jquants = JQuantsClient()
        self.yf = YFinanceFetcher()
        self.kabutan = KabutanScraper()
        self.edinetdb = EdinetDBClient()

    @staticmethod
    def _load_per_cache() -> Dict[str, float]:
        """PERキャッシュファイルを読み込む"""
        try:
            if PER_CACHE_PATH.exists():
                data = json.loads(PER_CACHE_PATH.read_text(encoding="utf-8"))
                cache = {}
                for code, val in data.get("data", {}).items():
                    if val is not None:
                        cache[code] = float(val)
                logger.info(f"PER cache loaded: {len(cache)} entries ({data.get('updated_at', '?')})")
                return cache
        except Exception as e:
            logger.warning(f"PER cache read error: {e}")
        return {}

    @staticmethod
    def _save_per_cache(per_map: Dict[str, float]):
        """PERキャッシュファイルを更新（既存値を保持しつつ新しい値で上書き）"""
        try:
            existing = {}
            if PER_CACHE_PATH.exists():
                raw = json.loads(PER_CACHE_PATH.read_text(encoding="utf-8"))
                existing = raw.get("data", {})
            # 既存値を保持しつつ、取得成功した値で上書き
            for code, val in per_map.items():
                if val:
                    existing[code] = round(val, 1)
            out = {
                "updated_at": datetime.date.today().isoformat(),
                "source": "kabutan (JP) / google_finance (US) / yfinance (US fallback)",
                "data": existing
            }
            PER_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            PER_CACHE_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info(f"PER cache saved: {len(existing)} entries")
        except Exception as e:
            logger.warning(f"PER cache save error: {e}")

    def _get_date_range(self):
        today = datetime.date.today()
        # データは今日まで取得（最新の株価をチャートに反映するため）
        end_date = today
        start_date = end_date - datetime.timedelta(weeks=REPORT_LOOKBACK_WEEKS)
        return start_date.isoformat(), end_date.isoformat()

    def _get_current_week_range(self):
        today = datetime.date.today()
        days_since_friday = (today.weekday() - 4) % 7
        end_date = today - datetime.timedelta(days=days_since_friday)
        start_date = end_date - datetime.timedelta(days=4)
        return start_date.isoformat(), end_date.isoformat()

    def fetch_jp_stock(self, code, from_date, to_date):
        self._current_to_date = to_date
        df = self.jquants.get_daily_quotes(code, from_date, to_date)
        if df.empty:
            logger.info(f"J-Quants empty for {code}, fallback to yfinance")
            df = self.yf.get_daily_data(f"{code}.T", from_date, to_date)
        # カブタンから最新日足を補完（J-Quants/yfinanceのデータ遅延対策）
        df = self._supplement_latest(df, code)
        return df

    def _supplement_latest(self, df: pd.DataFrame, code: str) -> pd.DataFrame:
        """J-Quants/yfinanceのデータに含まれていない最新日のOHLCVをカブタンから補完
        ただし取得期間の終了日から5営業日以上離れた日付は無視する"""
        latest = self.kabutan.get_latest_price(code)
        if not latest:
            return df
        latest_date = pd.Timestamp(latest["Date"])
        # 取得期間の終了日から大幅に離れた日付は補完しない（最大5営業日 = 7暦日）
        end_date = pd.Timestamp(getattr(self, '_current_to_date', '2099-12-31'))
        if latest_date > end_date + pd.Timedelta(days=7):
            logger.info(f"  {code}: Kabutan date {latest['Date']} too far from period end {self._current_to_date}, skipping")
            return df
        if df.empty:
            # データが全くない場合はカブタンのみで1行作成
            row = pd.DataFrame([{
                "Date": latest_date,
                "Open": latest["Open"], "High": latest["High"],
                "Low": latest["Low"], "Close": latest["Close"],
                "Volume": latest["Volume"],
            }])
            logger.info(f"  {code}: Kabutan only — {latest['Date']} ¥{latest['Close']:,.0f}")
            return row
        # 既存データの最新日
        last_date = df["Date"].max()
        if latest_date > last_date:
            row = pd.DataFrame([{
                "Date": latest_date,
                "Open": latest["Open"], "High": latest["High"],
                "Low": latest["Low"], "Close": latest["Close"],
                "Volume": latest["Volume"],
            }])
            df = pd.concat([df, row], ignore_index=True).sort_values("Date").reset_index(drop=True)
            logger.info(f"  {code}: +1 day from Kabutan ({latest['Date']} ¥{latest['Close']:,.0f})")
        else:
            logger.info(f"  {code}: data up to date ({last_date.date()})")
        return df

    @staticmethod
    def _get_prefetch_path() -> Path:
        """今日付きのChrome経由プリフェッチファイルパスを返す"""
        today = datetime.date.today().strftime("%Y%m%d")
        return Path(__file__).resolve().parent.parent / "cache" / f"chrome_prefetch_{today}.json"

    def _load_from_prefetch(self, path: Path, week_start: str, week_end: str, from_date: str, to_date: str) -> Dict:
        """Chrome経由で取得・保存されたプリフェッチJSONから fetch_all() と同じ構造のdictを生成する。
        プリフェッチ形式:
          {
            "jp_stocks":  { "325A": [[date,O,H,L,C,Vol], ...], ... },
            "topix":       [[date,O,H,L,C], ...],
            "indices":    { "Growth250": [[date,O,H,L,C], ...], "N225": [...] },
            "us_stocks":  { "ONON": [[date,O,H,L,C,Vol], ...], ... },
            "spx":         [[date,O,H,L,C], ...]
          }
        """
        logger.info(f"📂 Chrome prefetch found: {path.name} — skipping live API calls")
        raw = json.loads(path.read_text(encoding="utf-8"))

        def to_df(rows, cols):
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=cols)
            df["Date"] = pd.to_datetime(df["Date"])
            return df.sort_values("Date").reset_index(drop=True)

        ohlcv   = ["Date", "Open", "High", "Low", "Close", "Volume"]
        ohlc    = ["Date", "Open", "High", "Low", "Close"]

        result = {
            "tential":   pd.DataFrame(),
            "comps":     {},
            "benchmarks": {},
            "margin":    pd.DataFrame(),
            "metadata":  {
                "from_date": from_date, "to_date": to_date,
                "week_start": week_start, "week_end": week_end,
                "generated_at": datetime.datetime.now().isoformat(),
                "source": "chrome_prefetch",
            },
        }

        # JP stocks (J-Quants compact: [date, O, H, L, C, Vo])
        jp = raw.get("jp_stocks", {})
        tential_rows = jp.get("325A", [])
        result["tential"] = to_df(tential_rows, ohlcv)

        from config.settings import COMPS, BENCHMARKS
        name_to_code = {c.name: c.code for c in COMPS}
        for comp in COMPS:
            rows = jp.get(comp.code) or raw.get("us_stocks", {}).get(comp.code)
            if rows:
                cols = ohlcv if len(rows[0]) >= 6 else ohlc
                result["comps"][comp.name] = to_df(rows, cols[:len(rows[0])])
            else:
                result["comps"][comp.name] = pd.DataFrame()

        # Benchmarks
        bm_map = {
            "TOPIX":     (raw.get("topix",   []), ohlc),
            "グロース250": (raw.get("indices", {}).get("Growth250", []), ohlc),
            "日経平均":   (raw.get("indices", {}).get("N225",       []), ohlcv),
            "S&P 500":   (raw.get("spx",     []), ohlcv),
        }
        for bm_name, (rows, cols) in bm_map.items():
            result["benchmarks"][bm_name] = to_df(rows, cols[:len(rows[0])] if rows else cols)

        per_cache = self._load_per_cache()
        result["per_map"] = per_cache
        logger.info(f"✅ Prefetch loaded — TENTIAL: {len(result['tential'])} rows, "
                    f"comps: {len(result['comps'])}, benchmarks: {len(result['benchmarks'])}")
        return result

    def fetch_all(self) -> Dict:
        from_date, to_date = self._get_date_range()
        week_start, week_end = self._get_current_week_range()
        logger.info(f"Fetching: {from_date} → {to_date}, week: {week_start} → {week_end}")

        # ── Chrome プリフェッチ（VMプロキシをバイパス済みのデータ）があれば優先使用 ──
        prefetch_path = self._get_prefetch_path()
        if prefetch_path.exists():
            return self._load_from_prefetch(prefetch_path, week_start, week_end, from_date, to_date)

        result = {"tential": pd.DataFrame(), "comps": {}, "benchmarks": {},
                  "margin": pd.DataFrame(),
                  "metadata": {"from_date": from_date, "to_date": to_date,
                               "week_start": week_start, "week_end": week_end,
                               "generated_at": datetime.datetime.now().isoformat()}}

        logger.info(f"Fetching TENTIAL ({TENTIAL.code})...")
        result["tential"] = self.fetch_jp_stock(TENTIAL.code, from_date, to_date)

        for comp in COMPS:
            logger.info(f"Fetching {comp.name} ({comp.code})...")
            if comp.source == "jquants":
                result["comps"][comp.name] = self.fetch_jp_stock(comp.code, from_date, to_date)
            else:
                result["comps"][comp.name] = self.yf.get_daily_data(comp.yfinance_ticker, from_date, to_date)
            time.sleep(0.3)

        for name, info in BENCHMARKS.items():
            logger.info(f"Fetching benchmark: {name}...")
            df = pd.DataFrame()
            if info["source"] == "jquants":
                if info["code"] == "TOPIX":
                    df = self.jquants.get_topix(from_date, to_date)
                else:
                    df = self.jquants.get_indices(info["code"], from_date, to_date)
                if df.empty:
                    logger.info(f"  J-Quants empty for {name}, fallback to yfinance")
                    df = self.yf.get_daily_data(info["yfinance_ticker"], from_date, to_date)
            else:
                df = self.yf.get_daily_data(info["yfinance_ticker"], from_date, to_date)
            result["benchmarks"][name] = df
            logger.info(f"  {name}: {len(df)} rows")
            time.sleep(0.3)

        logger.info("Fetching margin data...")
        result["margin"] = self.jquants.get_margin_trading(TENTIAL.code, from_date, to_date)

        # PER — 3段フォールバック: カブタン/EDINET DB → yfinance(US) → キャッシュ
        logger.info("Fetching PER data...")
        per_map = {}
        per_cache = self._load_per_cache()

        # TENTIAL — カブタン → EDINET DB → キャッシュ
        per_val = self._fetch_jp_per(TENTIAL.code, per_cache)
        if per_val: per_map[TENTIAL.code] = per_val
        time.sleep(0.3)

        # Comps
        for comp in COMPS:
            if comp.source == "jquants":
                per_val = self._fetch_jp_per(comp.code, per_cache)
            else:
                # 米国株: yfinance → キャッシュ
                per_val = self.yf.get_per(comp.yfinance_ticker)
                src = "yfinance"
                if not per_val and comp.code in per_cache:
                    per_val = per_cache[comp.code]
                    src = "cache"
                if per_val:
                    logger.info(f"  {comp.name} PER: {per_val} ({src})")
                else:
                    logger.info(f"  {comp.name} PER: — (not available)")
            if per_val: per_map[comp.code] = per_val
            time.sleep(0.3)

        result["per_map"] = per_map
        # キャッシュ更新（取得成功した値で上書き）
        self._save_per_cache(per_map)

        logger.info("✅ Data fetch complete!")
        return result

    def _fetch_jp_per(self, code: str, cache: Dict[str, float]) -> float:
        """日本株PER取得: カブタン → キャッシュ の2段フォールバック
        ※ EDINET DBのPERは有報ベースEPSで算出されるため市場実勢と乖離が大きく、PER取得には使わない。
          EDINET DBは売上/利益/ROE等の財務データ取得に活用する。"""
        # 1. カブタン（市場実勢PER、最も正確）
        per_val = self.kabutan.get_per(code)
        if per_val:
            logger.info(f"  {code} PER: {per_val} (kabutan)")
            return per_val
        # 2. キャッシュ
        if code in cache:
            per_val = cache[code]
            logger.info(f"  {code} PER: {per_val} (cache)")
            return per_val
        logger.info(f"  {code} PER: — (not available)")
        return 0.0

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = StockDataFetcher().fetch_all()
    print(f"\nTENTIAL: {len(data['tential'])} rows")
    for n, df in data["comps"].items():
        print(f"  {n}: {len(df)} rows")
