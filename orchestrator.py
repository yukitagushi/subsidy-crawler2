import os, time
from lib.budget import set_monthly_limit, can_spend, add_usage
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search_vertex import discover as v_discover
from crawl_incremental import crawl as lane_crawl

VERTEX_Q_MONTH_LIMIT = int(os.getenv("VERTEX_Q_MONTH_LIMIT", "9000"))  # 無料枠1万の手前で止める
VERTEX_Q_PER_RUN     = int(os.getenv("VERTEX_Q_PER_RUN", "50"))        # 1Runあたりの見込み消費（pageSize×pages）

def main():
    t0=time.time()
    set_monthly_limit("vertex", VERTEX_Q_MONTH_LIMIT)

    try: lane_rss()
    except Exception as e: print("RSS lane error:", e)

    try:
        if can_spend("vertex", VERTEX_Q_PER_RUN):
            extra = v_discover(query="公募 補助金 申請 2025", page_size=25, max_pages=2)
            add_usage("vertex", VERTEX_Q_PER_RUN)
            print("vertex discovery candidates:", len(extra))
        else:
            print("vertex discovery skipped: monthly budget reached")
    except Exception as e:
        print("Vertex discovery error:", e)

    try: lane_crawl()
    except Exception as e: print("Crawl lane error:", e)

    print("Done in", int(time.time()-t0), "sec")

if __name__ == "__main__":
    main()
