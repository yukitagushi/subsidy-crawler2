import os
import time

from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.budget import set_monthly_limit, can_spend, add_usage
from lib.http_client import conditional_fetch
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover
from lanes.lane_search_openai import discover_and_extract as dr_discover
from crawl_incremental import crawl as lane_crawl

# ---- 月次クォータ ----
VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))
OPENAI_Q_MONTH_LIMIT = int(os.getenv("OPENAI_Q_MONTH_LIMIT", "9000"))
OPENAI_Q_PER_RUN     = int(os.getenv("OPENAI_Q_PER_RUN", "1"))

# ---- Discovery 切替 ----
USE_OPENAI_DR = os.getenv("USE_OPENAI_DR", "1") == "1"

# ---- ウォッチドッグ ----
HARD_KILL_SEC = int(os.getenv("HARD_KILL_SEC", "600"))  # 10min

# ---- 先読み件数（ENVで切替）----
PREFETCH_MAX = int(os.getenv("PREFETCH_MAX", "0"))      # 0=先読み停止

DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def time_left(deadline: float) -> float:
    return max(0.0, deadline - time.time())

def quick_prefetch(urls, max_n: int, deadline: float):
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

def print_run_summary(run_id: str):
    with conn() as c, c.cursor() as cur:
        # % を使わず POSITION で non-sentinel を数える
        cur.execute(
            "select count(*) from public.pages "
            "where position('https://example.com/sentinel' in url) = 0",
            (), prepare=False
        )
        pages_after = cur.fetchone()[0] or 0
        cur.execute(
            """
            select status, count(*)
              from public.fetch_log
             where position('run='||%s||';' in coalesce(error,'')) > 0
             group by status
            """,
            (run_id,), prepare=False
        )
        counts = {k: v for k, v in cur.fetchall()}
    print(f"SUMMARY run={run_id}: ok={counts.get('ok',0)}, 304={counts.get('304',0)}, "
          f"skip={counts.get('skip',0)}, ng={counts.get('ng',0)}, "
          f"list={counts.get('list',0)}, pages_non_sentinel={pages_after}")

def main():
    start = time.time()
    deadline = start + HARD_KILL_SEC
    ensure_schema()

    # 1) RSS（軽い）
    if time_left(deadline) < 5:
        print("watchdog: deadline before RSS"); return
    try:
        set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)
        set_monthly_limit("openai", OPENAI_Q_MONTH_LIMIT)
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # 2) crawl 本体（必ず実行）
    if time_left(deadline) < 5:
        print("watchdog: skip crawl (deadline reached before crawl)"); return
    try:
        lane_crawl()
    except Exception as e:
        print("Crawl lane error:", e)

    # 3) 残り時間で Discovery（OpenAI 優先）
    if time_left(deadline) < 5:
        print("watchdog: deadline before discovery")
        run_id = os.getenv("RUN_ID","")
        if run_id:
            try: print_run_summary(run_id)
            except Exception as e: print("summary error:", e)
        return

    try:
        saved_any = False

        if USE_OPENAI_DR and os.getenv("OPENAI_API_KEY",""):
            if can_spend("openai", OPENAI_Q_PER_RUN):
                default_queries = [
                    "補助金 公募 申請 2025",
                    "site:chusho.meti.go.jp 公募 2025",
                    "site:jgrants-portal.go.jp 公募 2025",
                    "site:meti.go.jp 公募 2025",
                ]
                qs = os.getenv("DR_QUERIES", "")
                queries = [q.strip() for q in qs.split("|") if q.strip()] or default_queries

                for q in queries:
                    if time_left(deadline) < 10:
                        break
                    items = dr_discover(query=q, max_items=int(os.getenv("DR_MAX_ITEMS","40")))
                    if items:
                        saved_any = True
                        break
                add_usage("openai", 1)
                print(f"openai dr saved_any={saved_any}")
            else:
                print("openai discovery skipped: monthly budget reached")

        # 0件なら Vertex へフォールバック
        if not saved_any:
            if can_spend("vertex", VERTEX_Q_PER_RUN):
                urls = v_discover(query="補助金 公募 申請 2025", page_size=25, max_pages=1)
                add_usage("vertex", VERTEX_Q_PER_RUN)
                print("vertex discovery candidates:", len(urls))
                if PREFETCH_MAX > 0:
                    quick_prefetch(urls, max_n=PREFETCH_MAX, deadline=deadline)
            else:
                print("vertex discovery skipped: monthly budget reached")

    except Exception as e:
        print("Discovery error:", e)

    # 4) サマリー（%不使用）
    run_id = os.getenv("RUN_ID","")
    if run_id:
        try: print_run_summary(run_id)
        except Exception as e: print("summary error:", e)

    print("Done in", int(time.time() - start), "sec")

if __name__ == "__main__":
    main()
