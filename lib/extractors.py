# lib/extractors.py — HTML から要点抽出（タイトル・要約・年度・補助率など）
from __future__ import annotations
import re
from bs4 import BeautifulSoup
from .util import norm_ws, clip

def _meta(soup: BeautifulSoup, *pairs: tuple[str, str]) -> str:
    """
    例: _meta(soup, ("property","og:title"), ("name","twitter:title"))
    上から順に探し、content を返す
    """
    for k, v in pairs:
        m = soup.find("meta", attrs={k: v})
        if m and m.get("content"):
            return norm_ws(m["content"])
    return ""


def extract_from_html(url: str, html: str) -> dict:
    """
    タイトルが取れないSPA等にも対応:
      - <title> → og:title → twitter:title → summary先頭 で補完
      - 年度: 和暦(令和x年度) / 西暦(2025年度) を両対応
      - 代表的なフィールドを緩い正規表現で抽出
    """
    soup = BeautifulSoup(html, "html.parser")

    # タイトル
    title = norm_ws(soup.title.text if soup.title else "") \
        or _meta(soup, ("property", "og:title"), ("name", "twitter:title"))

    # 要約（meta description → 先頭p）
    desc = _meta(soup, ("name", "description"), ("property", "og:description"))
    if desc:
        summary = desc
    else:
        first_p = soup.find("p")
        summary = norm_ws(first_p.get_text(" ")) if first_p else ""

    if not title:
        # タイトルが無いときは summary 先頭で補完
        title = (summary[:40] or "(無題)")

    # ページテキスト
    text = soup.get_text(" ")

    def f(pat: str) -> str | None:
        m = re.search(pat, text)
        if not m:
            return None
        g = m.group(1 if m.lastindex else 0)
        return norm_ws(g)

    # 年度（和暦 or 西暦）
    fiscal_year = f(r"(令和\s*[0-9０-９]+年度|20[0-9]{2}年度)")
    call_no     = f(r"第\s*([0-9０-９]+)\s*回")
    rate        = f(r"補助率[\s:：]*([0-9０-９]+ ?%?)")
    cap         = f(r"上限[\s:：]*([0-9０-９,，]+ ?(?:円|万円|億円)?)")

    # 対象/対象経費（見出し直後の行をラフに抜く）
    target, cost_items = None, None
    for lab in ("対象経費", "対象者", "対象"):
        m = re.search(lab + r"[\s:：]*(.+?)\n", text)
        if m:
            val = norm_ws(m.group(1))
            if "経費" in lab:
                cost_items = val
            else:
                target = val

    return {
        "url": url,
        "title": title or "(無題)",
        "summary": clip(summary, 800),
        "rate": rate,
        "cap": cap,
        "target": target,
        "cost_items": cost_items,
        "deadline": None,
        "fiscal_year": fiscal_year,
        "call_no": call_no,
        "scheme_type": None,
        "period_from": None,
        "period_to": None,
    }