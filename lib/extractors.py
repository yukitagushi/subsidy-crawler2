from __future__ import annotations
import re
from bs4 import BeautifulSoup
from .util import norm_ws, clip

def _meta(soup: BeautifulSoup, *pairs: tuple[str,str]) -> str:
    for k,v in pairs:
        m = soup.find("meta", attrs={k:v})
        if m and m.get("content"): return norm_ws(m["content"])
    return ""

def extract_from_html(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title = norm_ws(soup.title.text if soup.title else "") \
        or _meta(soup, ("property","og:title"), ("name","twitter:title"))
    desc  = _meta(soup, ("name","description"), ("property","og:description"))
    summary = desc or (norm_ws(soup.find("p").get_text(" ")) if soup.find("p") else "")
    if not title: title = (summary[:40] or "(無題)")
    text = soup.get_text(" ")

    def f(pat: str) -> str | None:
        m = re.search(pat, text)
        return norm_ws(m.group(1 if (m and m.lastindex) else 0)) if m else None

    fiscal_year = f(r"(令和\s*[0-9０-９]+年度|20[0-9]{2}年度)")
    call_no     = f(r"第\s*([0-9０-９]+)\s*回")
    rate        = f(r"補助率[\s:：]*([0-9０-９]+ ?%?)")
    cap         = f(r"上限[\s:：]*([0-9０-９,，]+ ?(?:円|万円|億円)?)")

    target, cost_items = None, None
    for lab in ("対象経費","対象者","対象"):
        m = re.search(lab + r"[\s:：]*(.+?)\n", text)
        if m:
            val = norm_ws(m.group(1))
            if "経費" in lab: cost_items = val
            else:             target     = val

    return {
        "url": url, "title": title or "(無題)", "summary": clip(summary, 800),
        "rate": rate, "cap": cap, "target": target, "cost_items": cost_items,
        "deadline": None, "fiscal_year": fiscal_year, "call_no": call_no,
        "scheme_type": None, "period_from": None, "period_to": None,
    }
