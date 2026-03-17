"""
GitHub Pages Publisher
HTMLレポートをdocs/にコピーしてindex.htmlのマニフェストを更新する
git push すれば自動でGitHub Pagesに反映される
"""
import json
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Paths
SRC_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = SRC_ROOT / "docs"
REPORTS_DIR = DOCS_DIR / "reports"
INDEX_PATH = DOCS_DIR / "index.html"


def deploy_to_pages(html_path: Path) -> Optional[str]:
    """
    HTMLレポートをGitHub Pages用docs/にデプロイ

    Args:
        html_path: 元のHTMLレポートのパス

    Returns:
        デプロイ先の相対パス（reports/weekly_report_YYYYMMDD.html）
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # HTMLをdocs/reports/にコピー
    dest_html = REPORTS_DIR / html_path.name
    shutil.copy2(html_path, dest_html)
    logger.info(f"Copied HTML → {dest_html}")

    # index.htmlのマニフェストを更新
    _update_manifest()

    rel_path = f"reports/{html_path.name}"
    logger.info(f"✅ GitHub Pages deploy ready: {rel_path}")
    return rel_path


def _scan_reports() -> List[Dict]:
    """docs/reports/ のHTMLファイルをスキャンしてマニフェストを生成"""
    reports = []
    for html_file in sorted(REPORTS_DIR.glob("weekly_report_*.html"), reverse=True):
        # ファイル名から日付を抽出: weekly_report_20260309.html → 2026-03-09
        m = re.search(r"weekly_report_(\d{4})(\d{2})(\d{2})", html_file.name)
        if m:
            y, mo, d = m.groups()
            date_str = f"{y}-{mo}-{d}"
            # 週の月曜日の日付を表示用に整形
            try:
                dt = datetime(int(y), int(mo), int(d))
                display_date = dt.strftime("%Y年%m月%d日（週）")
            except ValueError:
                display_date = date_str
        else:
            date_str = html_file.stem
            display_date = date_str

        entry = {
            "url": f"reports/{html_file.name}",
            "title": f"Weekly Report — {display_date}",
            "date": date_str,
            "filename": html_file.name,
        }

        reports.append(entry)

    return reports


def _update_manifest():
    """index.htmlのREPORTマニフェストを更新"""
    if not INDEX_PATH.exists():
        logger.warning("index.html not found — skipping manifest update")
        return

    reports = _scan_reports()
    manifest_json = json.dumps(reports, ensure_ascii=False, indent=2)

    content = INDEX_PATH.read_text(encoding="utf-8")
    updated = content.replace("__REPORT_MANIFEST__", manifest_json)

    # 既に展開済みの場合はJSONブロックを置換
    updated = re.sub(
        r'const REPORTS = \[.*?\];',
        f'const REPORTS = {manifest_json};',
        updated,
        flags=re.DOTALL,
    )

    INDEX_PATH.write_text(updated, encoding="utf-8")
    logger.info(f"Updated manifest: {len(reports)} reports")


def git_push_pages(commit_msg: Optional[str] = None) -> bool:
    """
    docs/の変更をgit commit & push（オプション）
    ローカルPCで実行する前提。サンドボックスではスキップ。
    """
    import subprocess

    if commit_msg is None:
        commit_msg = f"📊 Weekly report update — {datetime.now():%Y-%m-%d}"

    try:
        # docs/ のみステージ
        subprocess.run(["git", "add", "docs/"], cwd=str(SRC_ROOT), check=True)

        # 変更があるかチェック
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=str(SRC_ROOT),
            capture_output=True,
        )
        if result.returncode == 0:
            logger.info("No changes in docs/ — nothing to push")
            return True

        subprocess.run(
            ["git", "commit", "-m", commit_msg],
            cwd=str(SRC_ROOT),
            check=True,
        )
        subprocess.run(
            ["git", "push"],
            cwd=str(SRC_ROOT),
            check=True,
        )
        logger.info("✅ git push complete — GitHub Pages will update shortly")
        return True
    except FileNotFoundError:
        logger.warning("git not found — skipping push")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"git push failed: {e}")
        return False
