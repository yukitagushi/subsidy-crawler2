import os
import time

from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.budget import set_monthly_limit, can_spend, add_usage
from lib.http_client import conditional_fetch
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover
from lanes.lane_search_openai import discover_and_extract as dr_discover
from crawl_incremental import crawl as lane_crawl
from lib.extractors import extract_from_html

# ---- 月次クォータ ----
VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))
OPENAI_Q_MONTH_LIMIT = int(os.getenv("OPENAI_Q_MONTH_LIMIT", "9000"))
OPENAI_Q_PER_RUN     = int(os.getenv("OPENAI_Q_PER_RUN", "1"))

# ---- Discovery 切替 ----
USE_OPENAI_DR = os.getenv("USE_OPENAI_DR", "1") == "1"

# ---- ウォッチドッグ ----
HARD_KILL_SEC = int(os.getenv("HARD_KILL_SEC", "600"))  # 10min

# ---- 先読み件数（ENVで制御）----
PREFETCH_MAX = int(os.getenv("PREFETCH_MAX", "0"))      # 0=先読み停止（crawlに全振り）

# ---- backfill (未整備の種を本文取得) ----
BACKFILL_SEED_BATCH = int(os.getenv("BACKFILL_SEED_BATCH", "0"))  # 0=off

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
                changed = upsert_page(c, extract_from_html(u, html))
                log_fetch(c, u, "ok" if changed else "skip", took, "prefetch")
                if changed: taken += 1
            except Exception as e:
                log_fetch(c, u, "ng", 0, f"prefetch error: {e}")

def backfill_untitled(batch: int, deadline: float):
    """title='(無題)' または summary 空の URL を古い順に batch 件だけ本文取得して更新。"""
    if batch <= 0 or time_left(deadline) < 5:
        return

    urls = []
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            select url
            from public.pages
            where position('https://example.com/sentinel' in url)=0
              and (title='(無題)' or coalesce(summary,'')='')
            order by last_fetched asc nulls first
            limit %s
            """,
            (batch,),
            prepare=False
        )
        urls = [r[0] for r in cur.fetchall()]

    if not urls:
        return

    taken = 0
    with conn() as c:
        for u in urls:
            if time_left(deadline) < 5:
                break
            try:
                html, etag, lm, ctype, status, took = conditional_fetch(u, None, None)
                upsert_http_meta(c, u, etag, lm, status)
                if html is None:
                    log_fetch(c, u, "304", took, "backfill"); continue
                if ctype and ctype.lower() not in DOC_TYPES:
                    log_fetch(c, u, "skip", took, f"backfill ctype={ctype}"); continue
                changed = upsert_page(c, extract_from_html(u, html))
                log_fetch(c, u, "ok" if changed else "skip", took, "backfill")
                if changed: taken += 1
            except Exception as e:
                log_fetch(c, u, "ng", 0, f"backfill error: {e}")
    print(f"backfill updated: {taken}/{len(urls)}")

def seed_minimal_pages(urls, max_n: int = 100):
    """URL だけで pages に最小行を upsert（まず貯める運用）。"""
    def _row(url: str) -> dict:
        return {
            "url": url, "title": "(無題)", "summary": None,
            "rate": None, "cap": None, "target": None, "cost_items": None,
            "deadline": None, "fiscal_year": None, "call_no": None,
            "scheme_type": None, "period_from": None, "period_to": None
        }

    uniq, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
        if len(uniq) >= max_n: break

    saved = 0
    with conn() as c:
        for u in uniq:
            try:
                changed = upsert_page(c, _row(u))
                log_fetch(c, u, "seed" if changed else "skip", 0, "seed:minimal")
                if changed: saved += 1
            except Exception as e:
                log_fetch(c, u, "ng", 0, f"seed error: {e}")
    print(f"seeded minimal pages: {saved}/{len(uniq)}")

def print_run_summary(run_id: str):
    """% を一切使わず、POSITION と COALESCE で安全に集計。"""
    with conn() as c, c.cursor() as cur:
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
          f"list={counts.get('list',0)}, seed={counts.get('seed',0)}, "
          f"pages_non_sentinel={pages_after}")

def main():
    start = time.time()
    deadline = start + HARD_KILL_SEC
    ensure_schema()

    # 1) RSS
    if time_left(deadline) < 5: print("watchdog: deadline before RSS"); return
    try:
        set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)
        set_monthly_limit("openai", OPENAI_Q_MONTH_LIMIT)
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # 2) crawl 本体（本文取得で ok を狙う）
    if time_left(deadline) < 5: print("watchdog: skip crawl (deadline before crawl)"); return
    try:
        lane_crawl()
    except Exception as e:
        print("Crawl lane error:", e)

    # 3) backfill（(無題)/summary空を重点取得）
    if BACKFILL_SEED_BATCH > 0 and time_left(deadline) >= 5:
        backfill_untitled(BACKFILL_SEED_BATCH, deadline)

    # 4) 残り時間で Discovery（OpenAI 優先、ダメなら Vertex seed）
    if time_left(deadline) >= 5:
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
                        if time_left(deadline) < 10: break
                        items = dr_discover(query=q, max_items=int(os.getenv("DR_MAX_ITEMS","40")))
                        if items: saved_any = True; break
                    add_usage("openai", 1)
                    print(f"openai dr saved_any={saved_any}")
                else:
                    print("openai discovery skipped: monthly budget reached")

            if not saved_any:
                if can_spend("vertex", VERTEX_Q_PER_RUN):
                    urls = v_discover(query="補助金 公募 申請 2025", page_size=25, max_pages=1)
                    add_usage("vertex", VERTEX_Q_PER_RUN)
                    print("vertex discovery candidates:", len(urls))
                    if PREFETCH_MAX > 0:
                        quick_prefetch(urls, max_n=PREFETCH_MAX, deadline=deadline)
                    # seed でもDBに貯める
                    seed_minimal_pages(urls, max_n=100)
                else:
                    print("vertex discovery skipped: monthly budget reached")
        except Exception as e:
            print("Discovery error:", e)

    # 5) サマリー
    run_id = os.getenv("RUN_ID","")
    if run_id:
        try: print_run_summary(run_id)
        except Exception as e: print("summary error:", e)

    print("Done in", int(time.time() - start), "sec")

if __name__ == "__main__":
    main()
