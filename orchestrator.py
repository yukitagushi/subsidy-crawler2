import os
import time

from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.budget import set_monthly_limit, can_spend, add_usage
from lib.http_client import conditional_fetch
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover
from crawl_incremental import crawl as lane_crawl

# ---- 無料枠ガード（ENV で調整）----
VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))  # 月間の許容量（無料枠1万の手前）
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))        # 1Run あたり想定消費（pageSize×pages など）

# ---- ウォッチドッグ（この秒数を超えたら必ず main() を終了）----
HARD_KILL_SEC = int(os.getenv("HARD_KILL_SEC", "600"))  # 10分

# ---- 軽量抽出で保存できる Content-Type ----
DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def time_left(deadline: float) -> float:
    """終了までの残秒（負値は0に丸め）"""
    return max(0.0, deadline - time.time())

def quick_prefetch(urls, max_n: int = 2, deadline: float = float("inf")):
    """
    Discovery の候補を “少数だけ” 先に軽量取得して保存。
    3分ランでも確実に pages が増えるようにする“前座”。
    """
    taken = 0
    with conn() as c:
        for u in urls:
            if taken >= max_n or time_left(deadline) < 5:
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
    start = time.time()
    deadline = start + HARD_KILL_SEC

    # 1) スキーマ適用（api_quota の移行もここで走る）
    ensure_schema()

    # 2) RSS（軽いので先に）
    if time_left(deadline) < 5:
        print("watchdog: deadline before RSS"); return
    try:
        set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)  # 月次上限（無料枠）をセット
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # 3) ★ crawl 本体を先に回す（必ず少しでも取り込む）
    if time_left(deadline) < 5:
        print("watchdog: skip crawl (deadline reached before crawl)"); return
    try:
        lane_crawl()
    except Exception as e:
        print("Crawl lane error:", e)

    # 4) 残り時間があれば Discovery（searchLite）→ 先読み（prefetch）
    if time_left(deadline) < 5:
        print("watchdog: deadline before discovery"); return

    extra = []
    try:
        if can_spend("vertex", VERTEX_Q_PER_RUN):
            # 軽めに1ページだけ取得（最大25件）
            extra = v_discover(query="補助金 公募 申請 2025", page_size=25, max_pages=1)
            add_usage("vertex", VERTEX_Q_PER_RUN)
            print("vertex discovery candidates:", len(extra))
            # 先読みは 2 件だけ（短時間で完了するように）
            quick_prefetch(extra, max_n=2, deadline=deadline)
        else:
            print("vertex discovery skipped: monthly budget reached")
    except Exception as e:
        print("Vertex discovery error:", e)

    print("Done in", int(time.time() - start), "sec")

if __name__ == "__main__":
    main()
