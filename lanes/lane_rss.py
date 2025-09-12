import os, time, feedparser
from lib.db import conn, ensure_schema, log_fetch, upsert_page
from lib.util import norm_ws, clip

FEEDS = [
    # J-Net21 支援情報ヘッドライン（例：補助金系）
    "https://j-net21.smrj.go.jp/rss/support.xml",    # 代表例。複数あればここに追加
]

def ingest():
    ensure_schema()
    with conn() as c:
        for url in FEEDS:
            t0=time.time()
            try:
                d=feedparser.parse(url)
                for e in d.entries:
                    link = getattr(e, "link", None)
                    title = norm_ws(getattr(e, "title", "") or "")
                    summary = norm_ws(getattr(e, "summary", "") or "")
                    if not link: continue
                    row = {"url": link, "title": title or "(無題)", "summary": clip(summary, 800),
                           "rate": None, "cap": None, "target": None, "cost_items": None,
                           "deadline": None, "fiscal_year": None, "call_no": None,
                           "scheme_type": None, "period_from": None, "period_to": None}
                    changed = upsert_page(c, row)
                    log_fetch(c, link, "ok" if changed else "skip", 0, "rss")
            except Exception as e:
                log_fetch(c, url, "ng", int((time.time()-t0)*1000), f"rss error: {e}")
