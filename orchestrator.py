import os, time
from lib.budget import set_monthly_limit, can_spend, add_usage
from lib.http_client import conditional_fetch
from lib.db import conn, upsert_http_meta, upsert_page, log_fetch
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover
from crawl_incremental import crawl as lane_crawl

# 無料枠ガード（例：月9000 / 1Run=50）
VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))

DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def quick_prefetch(urls: list[str], max_n: int = 10):
    """Discoveryの候補を少数だけ先に軽量取得（http_cache更新＆すぐ保存できれば保存）。"""
    taken = 0
    with conn() as c:
        for u in urls:
            if taken >= max_n:
                break
            try:
                html, etag, lm, ctype, status, took = conditional_fetch(u, None, None)
                upsert_http_meta(c, u, etag, lm, status)
                if html is None:
                    log_fetch(c, u, "304", took, "prefetch")
                    continue
                if ctype and ctype.lower() not in DOC_TYPES:
                    log_fetch(c, u, "skip", took, f"prefetch ctype={ctype}")
                    continue

                # 簡易抽出（重いほうは crawl_incremental に任せる）
                from lib.extractors import extract_from_html
                row = extract_from_html(u, html)
                changed = upsert_page(c, row)
                log_fetch(c, u, "ok" if changed else "skip", took, "prefetch")
                taken += 1
            except Exception as e:
                log_fetch(c, u, "ng", 0, f"prefetch error: {e}")
                continue

def main():
    t0 = time.time()
    set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)

    # A: RSS
    try:
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # B: Discovery（searchLite）→ 予算内でだけ実行
    extra = []
    try:
        if can_spend("vertex", VERTEX_Q_PER_RUN):
            extra = v_discover(query="補助金 公募 2025", page_size=25, max_pages=2)
            add_usage("vertex", VERTEX_Q_PER_RUN)
            print("vertex discovery candidates:", len(extra))
            # まずは少数だけ即プリフェッチ（3分ランでも増えやすくする）
            quick_prefetch(extra, max_n=8)
        else:
            print("vertex discovery skipped: monthly budget reached")
    except Exception as e:
        print("Vertex discovery error:", e)

    # C: 並列クロール（anchors+regex+fallback）
    try:
        lane_crawl()
    except Exception as e:
        print("Crawl lane error:", e)

    print("Done in", int(time.time() - t0), "sec")

if __name__ == "__main__":
    main()
