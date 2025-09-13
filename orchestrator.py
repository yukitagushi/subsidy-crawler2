import os
import time

from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.budget import set_monthly_limit, can_spend, add_usage
from lib.http_client import conditional_fetch
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover
from crawl_incremental import crawl as lane_crawl

# ---- 無料枠ガード（ENV で調整）----
VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))

# ---- ウォッチドッグ（この秒数を超えたら必ず main() を終了）----
HARD_KILL_SEC = int(os.getenv("HARD_KILL_SEC", "600"))  # 10分

# ---- 軽量抽出で保存できる Content-Type ----
DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def time_left(deadline: float) -> float:
    return max(0.0, deadline - time.time())

def quick_prefetch(urls, max_n: int = 2, deadline: float = float("inf")):
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
    """ok 件数と pages 件数を安全に出力（%は使わない・NULL安全）"""
    with conn() as c, c.cursor() as cur:
        # pages 件数（sentinel除外）
        cur.execute(
            "select count(*) from public.pages where url not like 'https://example.com/sentinel%'", (),
            prepare=False
        )
        pages_after = cur.fetchone()[0] or 0

        # このRUNのステータス別件数（POSITION で部分一致）
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

        # candidates 合計
        cur.execute(
            """
            select coalesce(sum((regexp_match(coalesce(error,''),'candidates=([0-9]+)'))[1]::int),0)
              from public.fetch_log
             where status='list' and position('run='||%s||';' in coalesce(error,'')) > 0
            """,
            (run_id,), prepare=False
        )
        cand = cur.fetchone()[0] or 0

    print(f"SUMMARY run={run_id}: candidates={cand}, ok={counts.get('ok',0)}, "
          f"304={counts.get('304',0)}, skip={counts.get('skip',0)}, ng={counts.get('ng',0)}, "
          f"pages_non_sentinel={pages_after}")

def main():
    start = time.time()
    deadline = start + HARD_KILL_SEC

    # 1) スキーマ適用
    ensure_schema()

    # 2) RSS
    if time_left(deadline) < 5: print("watchdog: deadline before RSS"); return
    try:
        set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # 3) crawl 本体（必ず回す）
    if time_left(deadline) < 5: print("watchdog: skip crawl (deadline reached before crawl)"); return
    try:
        lane_crawl()
    except Exception as e:
        print("Crawl lane error:", e)

    # 4) 残り時間で Discovery → 先読み
    if time_left(deadline) < 5: print("watchdog: deadline before discovery"); return
    extra = []
    try:
        if can_spend("vertex", VERTEX_Q_PER_RUN):
            extra = v_discover(query="補助金 公募 申請 2025", page_size=25, max_pages=1)
            add_usage("vertex", VERTEX_Q_PER_RUN)
            print("vertex discovery candidates:", len(extra))
            quick_prefetch(extra, max_n=2, deadline=deadline)
        else:
            print("vertex discovery skipped: monthly budget reached")
    except Exception as e:
        print("Vertex discovery error:", e)

    # 5) サマリー（Run ID は Actions から渡される）
    run_id = os.getenv("RUN_ID","")
    if run_id:
        print_run_summary(run_id)

    print("Done in", int(time.time() - start), "sec")

if __name__ == "__main__":
    main()
