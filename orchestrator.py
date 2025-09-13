import os, time

from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.budget import set_monthly_limit, can_spend, add_usage
from lib.http_client import conditional_fetch
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover     # 既存（残す）
from lanes.lane_search_openai import discover_and_extract as dr_discover  # 新規
from crawl_incremental import crawl as lane_crawl

VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))
HARD_KILL_SEC        = int(os.getenv("HARD_KILL_SEC", "600"))
USE_OPENAI_DR        = os.getenv("USE_OPENAI_DR", "1") == "1"  # ← デフォルトON

DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def time_left(deadline: float) -> float:
    return max(0.0, deadline - time.time())

def main():
    start = time.time()
    deadline = start + HARD_KILL_SEC
    ensure_schema()

    # 1) RSS（軽い）
    if time_left(deadline) < 5: return
    try:
        set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # 2) crawl 本体（必ず回す）
    if time_left(deadline) < 5: return
    try:
        lane_crawl()
    except Exception as e:
        print("Crawl lane error:", e)

    # 3) 残り時間で Discovery（OpenAI DR 優先 / OFFなら Vertex）
    if time_left(deadline) < 5: return
    try:
        if USE_OPENAI_DR and os.getenv("OPENAI_API_KEY", ""):
            items = dr_discover(query="補助金 公募 申請 2025", max_items=20)
            print(f"openai dr candidates={len(items)}")
        else:
            if can_spend("vertex", VERTEX_Q_PER_RUN):
                urls = v_discover(query="補助金 公募 申請 2025", page_size=25, max_pages=1)
                add_usage("vertex", VERTEX_Q_PER_RUN)
                print("vertex discovery candidates:", len(urls))
            else:
                print("vertex discovery skipped: monthly budget reached")
    except Exception as e:
        print("Discovery error:", e)

    # 4) サマリはログに出す（%問題を避けるため）
    run_id = os.getenv("RUN_ID","")
    if run_id:
        try:
            with conn() as c, c.cursor() as cur:
                cur.execute(
                    "select count(*) from public.pages where url not like 'https://example.com/sentinel%'", (),
                    prepare=False
                )
                pages_after = cur.fetchone()[0] or 0
            print(f"SUMMARY run={run_id}: pages_non_sentinel={pages_after}")
        except Exception as e:
            print("summary error:", e)

    print("Done in", int(time.time() - start), "sec")

if __name__ == "__main__":
    main()
