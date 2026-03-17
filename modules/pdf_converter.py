"""
Module 6: HTML → PDF Converter
weasyprintを使ってHTMLレポートをPDFに変換
"""
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def convert_html_to_pdf(html_path: str | Path, pdf_path: str | Path = None) -> Path:
    """HTMLファイルをPDFに変換。
    pdf_path を省略すると、HTMLと同じディレクトリに同名.pdfで出力。
    """
    from weasyprint import HTML

    html_path = Path(html_path)
    if pdf_path is None:
        pdf_path = html_path.with_suffix(".pdf")
    else:
        pdf_path = Path(pdf_path)

    logger.info(f"Converting HTML → PDF: {html_path.name} → {pdf_path.name}")

    HTML(filename=str(html_path)).write_pdf(
        str(pdf_path),
        presentational_hints=True,
    )

    size_kb = pdf_path.stat().st_size / 1024
    logger.info(f"✅ PDF generated: {pdf_path.name} ({size_kb:.0f} KB)")
    return pdf_path
