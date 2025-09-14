import os
import time
import re
from urllib.parse import urlparse

from lib.db import ensure_schema, conn, upsert_http_meta, upsert_page, log_fetch
from lib.http_client import conditional_fetch
from lib.extractors import extract_from_html, norm_ws, clip
from lanes.lane_search_openai import dr_fetch_text  # ★追加：DRでURL本文を読む

# ==== シリアルモードのENV ====
HARD_KILL_SEC           = int(os.getenv("HARD_KILL_SEC", "600"))
SINGLE_BACKFILL_ONE     = os.getenv("SINGLE_BACKFILL_ONE", "0") == "1"
SINGLE_MAX_TRY          = int(os.getenv("SINGLE_MAX_TRY", "5"))
SINGLE_FORCE_READ_TIMEOUT = int(os.getenv("SINGLE_FORCE_READ_TIMEOUT", "0"))  # lib/http_client が参照
DR_FETCH_ON_SERIAL      = os.getenv("DR_FETCH_ON_SERIAL", "1") == "1"         # ★DRフォールバックON/OFF
RUN_ID = os.getenv("RUN_ID", "")

DOC_TYPES = {"text/html", "application/xhtml+xml", "application/pdf"}

def log_run(c, url: str, status: str, took_ms: int, msg: str | None):
    prefix = f"run={RUN_ID}; " if RUN_ID else ""
    log_fetch(c, url, status, took_ms, prefix + (msg or ""))

def time_left(deadline: float) -> float:
    return max(0.0, deadline - time.time())

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
    """DRで取ったテキストで title/summary を最低限埋める"""
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

def process_one(url: str) -> bool:
    """1件だけ処理。成功で True"""
    # 1) まずはHTTPでじっくり（SINGLE_FORCE_READ_TIMEOUT が効く）
    try:
        html, etag, lm, ctype, status, took = conditional_fetch(url, None, None)
        with conn() as c:
            upsert_http_meta(c, url, etag, lm, status)

        ct = (ctype or "").lower()
        if html is None:
            with conn() as c: log_run(c, url, "304", took, "single"); 
            # DRフォールバック
            if DR_FETCH_ON_SERIAL:
                txt = dr_fetch_text(url, max_chars=6000)
                if txt:
                    changed = _upsert_text_as_summary(url, txt)
                    with conn() as c: log_run(c, url, "ok" if changed else "skip", 0, "single dr-fetch (304)")
                    return changed
            return False

        # 2) HTMLだが meta refresh でPDFへ誘導
        if ct in ("text/html", "application/xhtml+xml"):
            m = re.search(r'http-equiv=["\']refresh["\'].*?url=([^";\']+\.pdf)', html, flags=re.I)
            if m:
                pdf_url = m.group(1)
                changed = False
                # PDFタイトルだけでも更新
                with conn() as c:
                    changed = upsert_page(c, _row_from_pdf(pdf_url))
                    log_run(c, url, "ok" if changed else "skip", took, "single html->pdf meta refresh")
                return changed
            # 通常のHTML抽出
            changed = False
            with conn() as c:
                changed = upsert_page(c, extract_from_html(url, html))
                log_run(c, url, "ok" if changed else "skip", took, f"single html ctype={ct} status={status}")
            return changed

        # 3) PDF なら最低限のタイトル更新
        if ct == "application/pdf":
            with conn() as c:
                changed = upsert_page(c, _row_from_pdf(url))
                log_run(c, url, "ok" if changed else "skip", took, f"single pdf status={status}")
            return changed

        # 4) それ以外：DRに投げて本文テキストを拾う
        if DR_FETCH_ON_SERIAL:
            txt = dr_fetch_text(url, max_chars=6000)
            if txt:
                changed = _upsert_text_as_summary(url, txt)
                with conn() as c: log_run(c, url, "ok" if changed else "skip", 0, f"single dr-fetch ctype={ct}")
                return changed

        with conn() as c: log_run(c, url, "skip", took, f"single ctype={ct} status={status}")
        return False

    except Exception as e:
        # HTTP自体に失敗 → DRフォールバック
        if DR_FETCH_ON_SERIAL:
            txt = dr_fetch_text(url, max_chars=6000)
            if txt:
                changed = _upsert_text_as_summary(url, txt)
                with conn() as c: log_run(c, url, "ok" if changed else "skip", 0, f"single dr-fetch after error: {type(e).__name__}")
                return changed
        with conn() as c: log_run(c, url, "ng", 0, f"single error: {type(e).__name__}: {e}")
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

def main():
    start = time.time()
    deadline = start + HARD_KILL_SEC

    if os.getenv("SINGLE_BACKFILL_ONE", "0") == "1":
        urls = pick_untitled_batch(max(1, int(os.getenv("SINGLE_MAX_TRY", "5"))))
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

    print("normal run path not used in SINGLE_BACKFILL_ONE mode")
    print_run_summary()
    print("Done in", int(time.time() - start), "sec")

if __name__ == "__main__":
    main()
