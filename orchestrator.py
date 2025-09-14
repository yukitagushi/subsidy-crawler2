import os
import sys
import time
import re
import argparse
import logging
import requests
from urllib.parse import urlparse

from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.http_client import conditional_fetch
from lib.extractors import extract_from_html, norm_ws, clip
from lanes.lane_search_openai import dr_fetch_text  # DRでURL本文を読む

# ==== シリアル/実行モード関連 ENV ====
HARD_KILL_SEC               = int(os.getenv("HARD_KILL_SEC", "600"))
SINGLE_BACKFILL_ONE         = os.getenv("SINGLE_BACKFILL_ONE", "0") == "1"   # 既定は無効
SINGLE_MAX_TRY              = int(os.getenv("SINGLE_MAX_TRY", "5"))
SINGLE_STAGE1_READ_TIMEOUT  = int(os.getenv("SINGLE_STAGE1_READ_TIMEOUT", "180"))  # ← 3分
DR_FETCH_ON_SERIAL          = os.getenv("DR_FETCH_ON_SERIAL", "1") == "1"

# RUN_ID: 未設定なら実行時刻で自動採番（サマリ集計のため常時付与）
RUN_ID = os.getenv("RUN_ID")
if not RUN_ID:
    RUN_ID = str(int(time.time()))

# 巨大判定（これ以上はDRへ）
LARGE_BYTES_THRESHOLD       = int(os.getenv("SINGLE_LARGE_BYTES", "8000000"))  # 8MB
HEAD_CONNECT_TIMEOUT        = int(os.getenv("HEAD_CONNECT_TIMEOUT", "8"))
HEAD_READ_TIMEOUT           = int(os.getenv("HEAD_READ_TIMEOUT", "6"))

# 取り回し上の既定
DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def time_left(deadline: float) -> float:
    return max(0.0, deadline - time.time())

def log_run(c, url: str, status: str, took_ms: int, msg: str | None):
    prefix = f"run={RUN_ID}; " if RUN_ID else ""
    log_fetch(c, url, status, took_ms, prefix + (msg or ""))

def _row_from_pdf(url: str) -> dict:
    title = os.path.basename(urlparse(url).path) or "(PDF)"
    title = title.replace(".pdf", "").replace(".PDF", "")
    return {
        "url": url,
        "title": f"{title} (PDF)",
        "summary": "PDF（本文未解析）",
        "rate": None, "cap": None, "target": None, "cost_items": None,
        "deadline": None, "fiscal_year": None, "call_no": None,
        "scheme_type": None, "period_from": None, "period_to": None
    }

def pick_untitled_batch(n: int) -> list[str]:
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
            (n,), prepare=False
        )
        return [r[0] for r in cur.fetchall()]

def _upsert_text_as_summary(url: str, text: str) -> bool:
    title = norm_ws(text.splitlines()[0] if text else "") or "(本文抜粋)"
    row = {
        "url": url, "title": title[:80],
        "summary": clip(norm_ws(text), 800),
        "rate": None, "cap": None, "target": None, "cost_items": None,
        "deadline": None, "fiscal_year": None, "call_no": None,
        "scheme_type": None, "period_from": None, "period_to": None
    }
    with conn() as c:
        return upsert_page(c, row)

def head_preflight(url: str):
    try:
        r = requests.head(
            url, allow_redirects=True,
            timeout=(HEAD_CONNECT_TIMEOUT, HEAD_READ_TIMEOUT),
            headers={"User-Agent":"Mozilla/5.0","Accept":"*/*"}
        )
        ct = (r.headers.get("Content-Type") or "").split(";")[0].lower()
        cl = r.headers.get("Content-Length")
        size = int(cl) if (cl and cl.isdigit()) else None
        return ct, size
    except Exception:
        return None, None

def process_one(url: str) -> bool:
    """
    1件だけ処理：
      0) HEADで軽く判定（PDF/巨大→DR or PDFタイトル更新）
      1) Stage1: READ=3分でGET。ReadTimeoutなら即DRに切替
      2) HTML抽出 or PDFタイトル更新
      3) 失敗/不足はDRで本文抽出
    """
    # 0) HEAD プリフライト
    ct_head, size_head = head_preflight(url)

    # PDF は本文GETせずタイトル更新
    if ct_head == "application/pdf":
        with conn() as c:
            changed = upsert_page(c, _row_from_pdf(url))
            log_run(c, url, "ok" if changed else "skip", 0, "single head: pdf")
        return changed

    # 巨大は DR 抽出
    if size_head and size_head >= LARGE_BYTES_THRESHOLD and DR_FETCH_ON_SERIAL:
        txt = dr_fetch_text(url, max_chars=6000)
        with conn() as c:
            if txt:
                changed = _upsert_text_as_summary(url, txt)
                log_run(c, url, "ok" if changed else "skip", 0, f"single head: large->{size_head} dr-fetch")
                return changed
            else:
                log_run(c, url, "skip", 0, f"single head: large->{size_head} dr-fetch none")

    # 1) Stage1 GET（READ=3分）
    try:
        html, etag, lm, ctype, status, took = conditional_fetch(
            url, None, None,
            override_connect=None,
            override_read=SINGLE_STAGE1_READ_TIMEOUT
        )
        with conn() as c:
            upsert_http_meta(c, url, etag, lm, status)

        ct = (ctype or "").lower()

        if html is None:
            # 304 → DR
            if DR_FETCH_ON_SERIAL:
                txt = dr_fetch_text(url, max_chars=6000)
                with conn() as c:
                    if txt:
                        changed = _upsert_text_as_summary(url, txt)
                        log_run(c, url, "ok" if changed else "skip", 0, "single dr-fetch (304)")
                        return changed
                    else:
                        log_run(c, url, "skip", 0, "single dr-fetch none (304)")
            return False

        if ct in ("text/html", "application/xhtml+xml"):
            # meta refresh → PDF
            m = re.search(r'http-equiv=["\']refresh["\'].*?url=([^";\']+\.pdf)', html, flags=re.I)
            if m:
                pdf_url = m.group(1)
                with conn() as c:
                    changed = upsert_page(c, _row_from_pdf(pdf_url))
                    log_run(c, url, "ok" if changed else "skip", took, "single html->pdf meta refresh")
                return changed

            # HTML抽出
            with conn() as c:
                changed = upsert_page(c, extract_from_html(url, html))
                log_run(c, url, "ok" if changed else "skip", took, f"single html stage1 status={status}")
            if not changed and DR_FETCH_ON_SERIAL:
                txt = dr_fetch_text(url, max_chars=6000)
                with conn() as c:
                    if txt:
                        changed = _upsert_text_as_summary(url, txt)
                        log_run(c, url, "ok" if changed else "skip", 0, "single dr-fetch after html")
                        return changed
                    else:
                        log_run(c, url, "skip", 0, "single dr-fetch none after html")
            return changed

        if ct == "application/pdf":
            with conn() as c:
                changed = upsert_page(c, _row_from_pdf(url))
                log_run(c, url, "ok" if changed else "skip", took, f"single pdf stage1")
            return changed

        # その他 → DR
        if DR_FETCH_ON_SERIAL:
            txt = dr_fetch_text(url, max_chars=6000)
            with conn() as c:
                if txt:
                    changed = _upsert_text_as_summary(url, txt)
                    log_run(c, url, "ok" if changed else "skip", 0, f"single dr-fetch ctype={ct}")
                    return changed
                else:
                    log_run(c, url, "skip", 0, f"single dr-fetch none ctype={ct}")
        return False

    except requests.exceptions.ReadTimeout:
        # ★ 3分でタイムアウト → 即DRへ
        if DR_FETCH_ON_SERIAL:
            txt = dr_fetch_text(url, max_chars=6000)
            with conn() as c:
                if txt:
                    changed = _upsert_text_as_summary(url, txt)
                    log_run(c, url, "ok" if changed else "skip", 0, "single stage1 ReadTimeout -> dr-fetch")
                    return changed
                else:
                    log_run(c, url, "skip", 0, "single stage1 ReadTimeout -> dr-fetch none")
        return False

    except Exception as e:
        # GET自体に失敗 → DR
        if DR_FETCH_ON_SERIAL:
            txt = dr_fetch_text(url, max_chars=6000)
            with conn() as c:
                if txt:
                    changed = _upsert_text_as_summary(url, txt)
                    log_run(c, url, "ok" if changed else "skip", 0, f"single dr-fetch after error: {type(e).__name__}")
                    return changed
                else:
                    log_run(c, url, "ng", 0, f"single error: {type(e).__name__}: {e}; dr-fetch none")
                    return False
        else:
            with conn() as c:
                log_run(c, url, "ng", 0, f"single error: {type(e).__name__}: {e}")
            return False

def print_run_summary():
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
            (RUN_ID,), prepare=False
        )
        counts = {k: v for k, v in cur.fetchall()}
    print(f"SUMMARY run={RUN_ID}: ok={counts.get('ok',0)}, 304={counts.get('304',0)}, "
          f"skip={counts.get('skip',0)}, ng={counts.get('ng',0)}, "
          f"list={counts.get('list',0)}, seed={counts.get('seed',0)}, "
          f"pages_non_sentinel={pages_after}")

# ========= 追加: 引数パース & 自己診断 & 簡易通常経路 =========

def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="orchestrator", add_help=True)
    sub = p.add_subparsers(dest="cmd")

    p_run = sub.add_parser("run", help="run a normal crawl lane")
    p_run.add_argument("--lane", choices=["serial","night","deep"], default="night")
    p_run.add_argument("--batch", type=int, default=int(os.getenv("BATCH_N","10")))
    p_run.add_argument("--fail-on-seed-zero", action="store_true")

    p_single = sub.add_parser("single", help="process a single url")
    p_single.add_argument("url")

    sub.add_parser("selfcheck", help="check env/DB connectivity and exit")
    return p.parse_args(argv)

def selfcheck() -> int:
    dsn = os.getenv("DATABASE_URL","")
    has_tv = bool(os.getenv("TAVILY_API_KEY"))
    allow_fb = os.getenv("ALLOW_FALLBACK","0") == "1"
    print(f"[SELF CHECK] DB_URL={'set' if dsn else 'missing'} TAVILY={has_tv} FALLBACK={allow_fb}")
    try:
        import psycopg
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("select 1")
            cur.fetchone()
        print("[SELF CHECK] DB ok")
        return 0
    except Exception as e:
        print(f"[SELF CHECK] DB error: {e}")
        return 2

def run_lane(lane: str, batch: int, deadline: float) -> tuple[int,int,int]:
    """
    簡易通常経路：未題/空要約ページを batch 件処理
    戻り値: (processed, ok_like, errors) ざっくり
    """
    urls = pick_untitled_batch(batch)
    processed = ok_like = errors = 0
    for u in urls:
        if time_left(deadline) < 5:
            break
        processed += 1
        try:
            if process_one(u):
                ok_like += 1
        except Exception:
            errors += 1
    return processed, ok_like, errors

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ensure_schema()  # 念のため（冪等）

    start = time.time()
    deadline = start + HARD_KILL_SEC

    args = parse_args(sys.argv[1:])

    # 自己診断
    if args.cmd == "selfcheck":
        rc = selfcheck()
        print_run_summary()
        print("Done in", int(time.time() - start), "sec")
        sys.exit(rc)

    # 単発URL
    if args.cmd == "single":
        _ = process_one(args.url)
        print_run_summary()
        print("Done in", int(time.time() - start), "sec")
        return

    # 簡易「通常」ラン
    if args.cmd == "run":
        processed, ok_like, errors = run_lane(args.lane, args.batch, deadline)
        if args.fail_on_seed_zero and processed == 0:
            logging.error("seed/対象なし（processed=0）")
            print_run_summary(); print("Done in", int(time.time() - start), "sec")
            sys.exit(3)
        print_run_summary()
        print("Done in", int(time.time() - start), "sec")
        return

    # 互換: 旧ENVベースのSINGLE_BACKFILL_ONE（指定時のみ）
    if SINGLE_BACKFILL_ONE:
        urls = pick_untitled_batch(max(1, SINGLE_MAX_TRY))
        updated = False
        for u in urls:
            if time_left(deadline) < 5: break
            if process_one(u):
                updated = True
                break
        if not urls: print("single: no untitled/empty-summary rows")
        elif not updated: print("single: tried but no update (ng/skip/304/dr)")
        print_run_summary()
        print("Done in", int(time.time() - start), "sec")
        return

    # どの経路にも合致しないとき（メッセージだけ出す）
    print("normal run path not implemented here. use: 'single <URL>' or 'run --lane night'")
    print_run_summary()
    print("Done in", int(time.time() - start), "sec")

if __name__ == "__main__":
    main()
