import re
from bs4 import BeautifulSoup
from .util import norm_ws, clip

def extract_from_html(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title = norm_ws(soup.title.text if soup.title else "")
    desc  = soup.find("meta", attrs={"name":"description"})
    summary = norm_ws(desc.get("content","")) if desc else ""
    if not summary:
        p = soup.find("p")
        summary = norm_ws(p.get_text(" ")) if p else ""
    h = soup.find(["h1","h2","h3"])
    if h and not title: title = norm_ws(h.get_text(" "))

    text = soup.get_text(" ")

    def f(p): 
        m = re.search(p, text)
        return norm_ws(m.group(1 if m and m.lastindex else 0)) if m else None

    rate = f(r"補助率[\s:：]*([0-9０-９]+ ?%?)")
    cap  = f(r"上限[\s:：]*([0-9０-９,，]+ ?(?:円|万円|億円)?)")
    fy   = f(r"(令和\s*[0-9０-９]+年度)")
    cn   = f(r"第\s*([0-9０-９]+)回")

    target, cost_items = None, None
    for lab in ("対象経費","対象者","対象"):
        m = re.search(lab + r"[\s:：]*(.+?)\n", text)
        if m:
            val = norm_ws(m.group(1))
            if "経費" in lab: cost_items = val
            else: target = val

    return dict(
      url=url, title=title or "(無題)", summary=clip(summary, 800),
      rate=rate, cap=cap, target=target, cost_items=cost_items,
      deadline=None, fiscal_year=fy, call_no=cn, scheme_type=None,
      period_from=None, period_to=None
    )