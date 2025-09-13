import os, time
from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.budget import set_monthly_limit, can_spend, add_usage
from lib.http_client import conditional_fetch
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover
from crawl_incremental import crawl as lane_crawl

# 無料枠ガード
VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))

DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def quick_prefetch(urls, max_n=8):
    """Discovery候補から少数だけ先に保存: 3分ランでも確実に増やすための前座。"""
    taken = 0
    with conn() as c:
        for u in urls:
            if taken >= max_n:
                break
            try:
                html, etag, lm, ctype, status, took = conditional_fetch(u, None, None)
                upsert_http_meta(c, u, etag, lm, status)
                if html is None:
                    log_fetch(c, u, "304", took, "prefetch"); continue
                if ctype and ctype.lower() not in DOC_TYPES:
                    log_fetch(c, u, "skip", took, f"prefetch ctype={ctype}"); continue
                from lib.extractors import extract_from_html
                changed = upsert_page(c, extract_from_html(u, html))
                log_fetch(c, u, "ok" if changed else "skip", took, "prefetch")
                if changed: taken += 1
            except Exception as e:
                log_fetch(c, u, "ng", 0, f"prefetch error: {e}")

def main():
    t0 = time.time()

    # ★ まずスキーマを適用（api_quota のリネーム等もここで走る）
    ensure_schema()

    # A: RSS
    try:
        set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # B: Discovery（searchLite）→ 予算に余裕がある時だけ実行
    extra = []
    try:
        if can_spend("vertex", VERTEX_Q_PER_RUN):
            extra = v_discover(query="補助金 公募 申請 2025", page_size=25, max_pages=2)
            add_usage("vertex", VERTEX_Q_PER_RUN)
            print("vertex discovery candidates:", len(extra))
            quick_prefetch(extra, max_n=8)
        else:
            print("vertex discovery skipped: monthly budget reached")
    except Exception as e:
        print("Vertex discovery error:", e)

    # C: 並列クロール
    try:
        lane_crawl()
    except Exception as e:
        print("Crawl lane error:", e)

    print("Done in", int(time.time() - t0), "sec")

if __name__ == "__main__":
    main()
