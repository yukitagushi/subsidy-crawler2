import time
from lanes.lane_rss import ingest as lane_rss
from lanes.lane_search import discover as lane_search
from crawl_incremental import crawl as lane_crawl  # 既存並列版

def main():
    t0=time.time()
    # A: RSS（最軽量）
    try:
        lane_rss()
    except Exception as e:
        print("RSS lane error:", e)

    # B: 検索API（候補だけ増やしておく）
    extra = []
    try:
        extra = lane_search(max_results=60)
        print("search candidates:", len(extra))
    except Exception as e:
        print("Search lane error:", e)

    # C: 並列クロール（seeds + anchors/regex + discovery + fallback）
    #   ここでは seeds.yaml ベースに回す。extra候補は http_cache に入っていれば 304 で軽く流れる。
    try:
        lane_crawl()   # 必要なら lane_crawl(extra_candidates=extra) に拡張可
    except Exception as e:
        print("Crawl lane error:", e)

    print("Done in", int(time.time()-t0), "sec")

if __name__ == "__main__":
    main()
