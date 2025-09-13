import os
import time

from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.budget import set_monthly_limit, can_spend, add_usage
from lib.http_client import conditional_fetch
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover
from lanes.lane_search_openai import discover_and_extract as dr_discover  # ← OpenAI 版
from crawl_incremental import crawl as lane_crawl

# ---- 月次クォータ（ENV で調整）----
VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))
OPENAI_Q_MONTH_LIMIT = int(os.getenv("OPENAI_Q_MONTH_LIMIT", "9000"))
OPENAI_Q_PER_RUN     = int(os.getenv("OPENAI_Q_PER_RUN", "1"))

# ---- Deep Research を使うか ----
USE_OPENAI_DR = os.getenv("USE_OPENAI_DR", "1") == "1"

# ---- ウォッチドッグ（この秒数を超えたら必ず main() を終了）----
HARD_KILL_SEC = int(os.getenv("HARD_KILL_SEC", "600"))  # 10分

# ---- 軽量抽出で保存できる Content-Type ----
DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def time_left(deadline: float) -> float:
    return max(0.0, deadline - time.time())

def print_run_summary(run_id: str):
    """ok 件数と pages 件数をログに出す（SQLに % を使わない）"""
    with conn() as c, c.cursor() as cur:
        cur.execute(
            "select count(*) from public.pages where url not like 'https://example.com/sentinel%'", (),
            prepare=False
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

    # 1) スキーマ適用（api_quota の移行もここで走る）
    ensure_schema()

    # 2) RSS（軽い）
    if time_left(deadline) < 5: 
        print("watchdog: deadline before RSS"); 
        return
    try:
        # 月次上限（vertex/openai）を設定（初回／月替わりで上書き）
        set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)
        set_monthly_limit("openai", OPENAI_Q_MONTH_LIMIT)
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # 3) crawl 本体（必ず回す）
    if time_left(deadline) < 5: 
        print("watchdog: skip crawl (deadline reached before crawl)"); 
        return
    try:
        lane_crawl()
    except Exception as e:
        print("Crawl lane error:", e)

    # 4) 残り時間で Discovery（OpenAI 優先 / OFFなら Vertex）
    if time_left(deadline) < 5: 
        print("watchdog: deadline before discovery"); 
        return

    try:
        if USE_OPENAI_DR and os.getenv("OPENAI_API_KEY", ""):
            if can_spend("openai", OPENAI_Q_PER_RUN):
                items = dr_discover(query="補助金 公募 申請 2025", max_items=20)
                add_usage("openai", OPENAI_Q_PER_RUN)
                print(f"openai dr candidates={len(items)} (saved via upsert_page)")
            else:
                print("openai discovery skipped: monthly budget reached")
        else:
            if can_spend("vertex", VERTEX_Q_PER_RUN):
                urls = v_discover(query="補助金 公募 申請 2025", page_size=25, max_pages=1)
                add_usage("vertex", VERTEX_Q_PER_RUN)
                print("vertex discovery candidates:", len(urls))
            else:
                print("vertex discovery skipped: monthly budget reached")
    except Exception as e:
        print("Discovery error:", e)

    # 5) サマリー（Run ID は Actions から渡される）
    run_id = os.getenv("RUN_ID","")
    if run_id:
        try:
            print_run_summary(run_id)
        except Exception as e:
            print("summary error:", e)

    print("Done in", int(time.time() - start), "sec")

if __name__ == "__main__":
    main()
