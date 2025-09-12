from __future__ import annotations
import re
from bs4 import BeautifulSoup
from .util import norm_ws, clip

def _meta(soup: BeautifulSoup, *pairs: tuple[str, str]) -> str:
    """meta(name=...|property=...) から content を拾う"""
    for k, v in pairs:
        m = soup.find("meta", attrs={k: v})
        if m and m.get("content"):
            return norm_ws(m["content"])
    return ""

def extract_from_html(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # タイトル：<title> → og:title → twitter:title → summary先頭で補完
    title = norm_ws(soup.title.text if soup.title else "") \
        or _meta(soup, ("property", "og:title"), ("name", "twitter:title"))

    desc = _meta(soup, ("name", "description"), ("property", "og:description"))
    if desc:
        summary = desc
    else:
        p = soup.find("p")
        summary = norm_ws(p.get_text(" ")) if p else ""

    if not title:
        title = (summary[:40] or "(無題)")

    text = soup.get_text(" ")

    def f(pat: str) -> str | None:
        m = re.search(pat, text)
        if not m:
            return None
        g = m.group(1 if m.lastindex else 0)
        return norm_ws(g)

    fiscal_year = f(r"(令和\s*[0-9０-９]+年度|20[0-9]{2}年度)")
    call_no     = f(r"第\s*([0-9０-９]+)\s*回")
    rate        = f(r"補助率[\s:：]*([0-9０-９]+ ?%?)")
    cap         = f(r"上限[\s:：]*([0-9０-９,，]+ ?(?:円|万円|億円)?)")

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

# ---- フォールバック用：プレーンテキストから最低限抽出 ----
def extract_from_text(url: str, text: str) -> dict:
    t = norm_ws(text or "")
    title = "(無題)"
    m = re.search(r"^(.{8,80})$", t, flags=re.M)
    if m:
        title = norm_ws(m.group(1))

    rate = None
    m = re.search(r"補助率[\s:：]*([0-9０-９]+ ?%?)", t)
    if m:
        rate = norm_ws(m.group(1))

    cap = None
    m = re.search(r"上限[\s:：]*([0-9０-９,，]+ ?(?:円|万円|億円)?)", t)
    if m:
        cap = norm_ws(m.group(1))

    fiscal_year = None
    m = re.search(r"(令和\s*[0-9０-９]+年度|20[0-9]{2}年度)", t)
    if m:
        fiscal_year = norm_ws(m.group(1))

    return {
        "url": url,
        "title": title or "(無題)",
        "summary": clip(t[:800], 800),
        "rate": rate,
        "cap": cap,
        "target": None,
        "cost_items": None,
        "deadline": None,
        "fiscal_year": fiscal_year,
        "call_no": None,
        "scheme_type": None,
        "period_from": None,
        "period_to": None,
    }