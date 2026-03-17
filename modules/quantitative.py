"""
Module 2: Quantitative Analysis
パフォーマンス比較・需給分析
"""
import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
import pandas as pd, numpy as np
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import TENTIAL, COMPS, CATEGORY_ORDER

@dataclass
class DailyBar:
    """日足1本分のOHLCV"""
    date: str = ""; open: float = 0.0; high: float = 0.0
    low: float = 0.0; close: float = 0.0; volume: int = 0

@dataclass
class StockPerformance:
    name: str; code: str; category: str
    close: float = 0.0; weekly_return: float = 0.0
    mtd_return: float = 0.0; ytd_return: float = 0.0
    weekly_high: float = 0.0; weekly_low: float = 0.0
    avg_turnover: float = 0.0; turnover_change: float = 0.0
    avg_volume: int = 0           # 週平均出来高
    per: float = 0.0              # PER（株価収益率）
    daily_bars: List[DailyBar] = field(default_factory=list)   # 日足データ
    weekly_bars: List[DailyBar] = field(default_factory=list)   # 週足データ
    monthly_bars: List[DailyBar] = field(default_factory=list)  # 月足データ

@dataclass
class CategoryPerformance:
    category: str; avg_weekly_return: float = 0.0
    avg_mtd_return: float = 0.0; avg_ytd_return: float = 0.0
    best_performer: str = ""; worst_performer: str = ""

@dataclass
class MarginData:
    long_balance: int = 0; short_balance: int = 0
    long_change_pct: float = 0.0; short_change_pct: float = 0.0; ratio: float = 0.0

@dataclass
class QuantReport:
    tential: StockPerformance = None
    comps: List[StockPerformance] = field(default_factory=list)
    categories: List[CategoryPerformance] = field(default_factory=list)
    benchmarks: Dict[str, StockPerformance] = field(default_factory=dict)
    margin: MarginData = field(default_factory=MarginData)
    weekly_prices: Dict[str, pd.DataFrame] = field(default_factory=dict)

class QuantitativeAnalyzer:
    def __init__(self, data: Dict):
        self.data = data
        self.metadata = data["metadata"]

    def _calc_return(self, df, start, end):
        if df.empty or "Close" not in df.columns: return 0.0
        mask = (df["Date"] >= pd.Timestamp(start)) & (df["Date"] <= pd.Timestamp(end))
        p = df[mask].sort_values("Date")
        if len(p) < 2: return 0.0
        s, e = p.iloc[0]["Close"], p.iloc[-1]["Close"]
        return (e/s - 1)*100 if s else 0.0

    def _build_perf(self, name, code, cat, df):
        perf = StockPerformance(name=name, code=code, category=cat)
        if df.empty: return perf
        perf.close = df.iloc[-1]["Close"] if "Close" in df.columns else 0.0
        perf.weekly_return = self._calc_return(df, self.metadata["week_start"], self.metadata["week_end"])
        end = self.metadata["to_date"]
        perf.mtd_return = self._calc_return(df, end[:8]+"01", end)
        perf.ytd_return = self._calc_return(df, end[:5]+"01-01", end)
        # Weekly range
        mask = (df["Date"] >= pd.Timestamp(self.metadata["week_start"])) & (df["Date"] <= pd.Timestamp(self.metadata["week_end"]))
        w = df[mask]
        if not w.empty and "High" in w.columns:
            perf.weekly_high, perf.weekly_low = w["High"].max(), w["Low"].min()
        # Turnover & Volume
        tv_col = "TurnoverValue" if "TurnoverValue" in df.columns else None
        if not tv_col and "Volume" in df.columns and "Close" in df.columns:
            df = df.copy(); df["TurnoverValue"] = df["Volume"]*df["Close"]; tv_col = "TurnoverValue"
        if tv_col:
            end_ts, start_ts = pd.Timestamp(self.metadata["week_end"]), pd.Timestamp(self.metadata["week_start"])
            this = df[(df["Date"]>=start_ts)&(df["Date"]<=end_ts)][tv_col]
            prev = df[(df["Date"]>=start_ts-pd.Timedelta(days=7))&(df["Date"]<start_ts)][tv_col]
            a, b = this.mean() if len(this) else 0, prev.mean() if len(prev) else 0
            perf.avg_turnover = a
            perf.turnover_change = (a/b-1)*100 if b else 0.0
        # Average volume (weekly)
        if "Volume" in df.columns and not w.empty:
            perf.avg_volume = int(w["Volume"].mean())
        # Daily bars (full period for chart)
        if all(c in df.columns for c in ["Open","High","Low","Close"]):
            bars_df = df.sort_values("Date")
            vol_col = "Volume" if "Volume" in bars_df.columns else None
            for _, row in bars_df.iterrows():
                perf.daily_bars.append(DailyBar(
                    date=row["Date"].strftime("%m/%d") if hasattr(row["Date"], "strftime") else str(row["Date"]),
                    open=row["Open"], high=row["High"],
                    low=row["Low"], close=row["Close"],
                    volume=int(row[vol_col]) if vol_col and pd.notna(row[vol_col]) else 0,
                ))
            # Weekly / Monthly bars — 日足を集約
            perf.weekly_bars = self._aggregate_period(bars_df, "W", vol_col)
            perf.monthly_bars = self._aggregate_period(bars_df, "M", vol_col)
        # PER — data dict 内の per_map から取得
        per_map = self.data.get("per_map", {})
        if code in per_map:
            perf.per = per_map[code]
        return perf

    @staticmethod
    def _aggregate_period(df: pd.DataFrame, freq: str, vol_col: str = None) -> List[DailyBar]:
        """日足DataFrameを指定期間に集約 (freq: 'W'=週足, 'M'=月足)"""
        df = df.copy().sort_values("Date")
        df["_period"] = df["Date"].dt.to_period(freq)
        bars = []
        for period, grp in df.groupby("_period"):
            # ラベル: 週足は開始日(MM/DD), 月足は YYYY-MM
            if freq == "W":
                label = grp.iloc[0]["Date"].strftime("%m/%d")
            else:
                label = str(period)
            bars.append(DailyBar(
                date=label,
                open=grp.iloc[0]["Open"],
                high=grp["High"].max(),
                low=grp["Low"].min(),
                close=grp.iloc[-1]["Close"],
                volume=int(grp[vol_col].sum()) if vol_col and vol_col in grp.columns else 0,
            ))
        return bars

    def analyze(self):
        report = QuantReport()
        report.tential = self._build_perf(TENTIAL.name, TENTIAL.code, "自社", self.data["tential"])
        for c in COMPS:
            df = self.data["comps"].get(c.name, pd.DataFrame())
            report.comps.append(self._build_perf(c.name, c.code, c.category, df))
        # Categories
        for cat in CATEGORY_ORDER:
            if cat == "自社": continue
            stocks = [p for p in report.comps if p.category == cat]
            if not stocks: continue
            cp = CategoryPerformance(category=cat)
            cp.avg_weekly_return = np.mean([s.weekly_return for s in stocks])
            cp.avg_mtd_return = np.mean([s.mtd_return for s in stocks])
            cp.avg_ytd_return = np.mean([s.ytd_return for s in stocks])
            s = sorted(stocks, key=lambda x: x.weekly_return)
            cp.worst_performer, cp.best_performer = s[0].name, s[-1].name
            report.categories.append(cp)
        # Benchmarks
        for name, df in self.data["benchmarks"].items():
            report.benchmarks[name] = self._build_perf(name, name, "ベンチマーク", df)
        # Margin — V2: LongVol(買い残), ShrtVol(売り残)
        mdf = self.data.get("margin", pd.DataFrame())
        m = MarginData()
        if len(mdf) >= 2:
            la, pr = mdf.iloc[-1], mdf.iloc[-2]
            def _get_val(row, keys):
                for k in keys:
                    if k in row.index and row[k] is not None:
                        try: return int(float(row[k]))
                        except: pass
                return 0
            long_keys = ["LongVol", "long_balance", "LongBalance", "MarginBuyingBalance"]
            short_keys = ["ShrtVol", "short_balance", "ShortBalance", "MarginSellingBalance"]
            m.long_balance = _get_val(la, long_keys)
            m.short_balance = _get_val(la, short_keys)
            pl = _get_val(pr, long_keys)
            ps = _get_val(pr, short_keys)
            m.long_change_pct = (m.long_balance/pl-1)*100 if pl else 0
            m.short_change_pct = (m.short_balance/ps-1)*100 if ps else 0
            m.ratio = m.long_balance/m.short_balance if m.short_balance else 0
        report.margin = m
        return report
