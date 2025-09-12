# crawl_incremental.py — 並列取得（max_workers）＋ホストごと2並列制限
# 条件付きGET / DocOnly / Content-Type判定 / Tavily discovery / Tavily raw fallback
from __future__ import annotations

import os
import re
import time
import threading
from typing import List, Set
from urllib.parse import urlsplit, urljoin

import yaml
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.http_client import conditional_fetch
from lib.extractors import extract_from_html, extract_from_text
from lib.db import conn, upsert_http_meta, upsert_page, log_fetch, ensure_schema

# ----- Tavily (任意：Secretsに TAVILY_API_KEY がある場合のみ有効) -----
try:
    from tavily import TavilyClient
except Exception:
    TavilyClient = None

TAVILY_KEY = os.getenv("TAVILY_API_KEY")
tv = TavilyClient(api_key=TAVILY_KEY) if (TAVILY_KEY and TavilyClient) else None

# -------- ENV（必要に応じて crawl.yml で調整） --------
TIME_BUDGET_SEC   = int(os.getenv("TIME_BUDGET_SEC", "480"))
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN", "120"))
MAX_PER_DOMAIN    = int(os.getenv("MAX_PER_DOMAIN", "50"))

# 並列関連
PARALLEL_WORKERS  = int(os.getenv("PARALLEL_WORKERS", "6"))  # 総スレッド数
PER_HOST_LIMIT    = int(os.getenv("PER_HOST_LIMIT", "2"))    # ホストごとの同時数

# discovery
USE_TAVILY_DISCOVERY = os.getenv("USE_TAVILY_DISCOVERY", "1") == "1"

# 受け入れる Content-Type（本文とみなす）
DOC_TYPES: Set[str] = {"text/html", "application/xhtml+xml", "application/pdf"}

# アセットURLを除外（js/css/画像/フォント等）
ASSET_RE = re.compile(
    r'\.(js|mjs|css|png|jpe?g|gif|svg|ico|json|map|woff2?|ttf|eot|mp4|webm)($|\?)',
    re.I,
)

ALLOWED_HOSTS: Set[str] = set()


def allowed(u: str) -> bool:
    try:
        host = urlsplit(u).netloc
    except Exception:
        return False
    return any(host == d or host.endswith(d) for d in ALLOWED_HOSTS)


def is_document_url(u: str) -> bool:
    if not u.startswith(("http://", "https://")):
        return False
    if ASSET_RE.search(u):
        return False
    return True


def load_seeds(path: str = "seeds.yaml") -> List[dict]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    global ALLOWED_HOSTS
    ALLOWED_HOSTS = set(cfg.get("allowed_hosts", []))
    return cfg.get("sources", [])


def extract_links(base_url: str, html: str) -> List[str]:
    """a[href] だけを対象に抽出。script/link等は拾わない。"""
    soup = BeautifulSoup(html, "html.parser")
    out: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        out.append(urljoin(base_url, href))
    # 重複除外（順序維持）
    seen = set()
    uniq: List[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def discover_with_tavily(source: dict) -> List[str]:
    """
    Tavily で allowed_hosts のドメインに限定して候補URLを発見。
    seeds.yaml で discover: "tavily" / query: "..." を指定可能。
    """
    if not tv or not USE_TAVILY_DISCOVERY:
        return []
    include_domains = list(ALLOWED_HOSTS)
    q = source.get("query") or "補助金 公募 申請"
    try:
        r = tv.search(
            q,
            search_depth="advanced",
            max_results=int(source.get("max_new", 20)),
            include_domains=include_domains,
            include_answer=False,
            include_raw_content=False,
        )
    except Exception:
        return []
    out: List[str] = []
    for x in r.get("results", []):
        u = (x or {}).get("url") or ""
        if u and allowed(u) and is_document_url(u):
            out.append(u)
    # 重複除外
    seen = set()
    uniq: List[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


# ------- 並列用の共有状態（カウンタ・セマフォ） -------
_saved_count = 0
_saved_lock = threading.Lock()

host_semaphores: dict[str, threading.Semaphore] = {}
host_sem_lock = threading.Lock()


def _host_sem(host: str) -> threading.Semaphore:
    with host_sem_lock:
        sem = host_semaphores.get(host)
        if sem is None:
            sem = threading.Semaphore(PER_HOST_LIMIT)
            host_semaphores[host] = sem
        return sem


def _inc_saved(n: int = 1) -> None:
    global _saved_count
    with _saved_lock:
        _saved_count += n


# ------- 詳細ページ処理（スレッドワーカー） -------
def process_detail(u: str, deadline: float) -> None:
    """単一URLを処理。DBは各スレッドが都度 open/close する。"""
    if time.time() > deadline:
        # 期限超過は skip としてログ
        with conn() as c:
            log_fetch(c, u, "skip", 0, "deadline")
        return

    host = urlsplit(u).netloc
    sem = _host_sem(host)

    with sem:  # ホストごとの同時実行を制限
        try:
            with conn() as c:
                # 既存ETag/LM
                cur = c.cursor()
                cur.execute(
                    "select etag, last_modified from public.http_cache where url=%s",
                    (u,),
                )
                petag, plm = cur.fetchone() or (None, None)

                # 条件付きGET
                html, new_etag, new_lm, ctype, status, took = conditional_fetch(
                    u, petag, plm
                )
                upsert_http_meta(c, u, new_etag, new_lm, status)

                if html is None:
                    log_fetch(c, u, "304", took, None)
                    return
                if ctype and ctype.lower() not in DOC_TYPES:
                    log_fetch(c, u, "skip", took, f"ctype={ctype}")
                    return

                row = extract_from_html(u, html)
                changed = upsert_page(c, row)
                log_fetch(c, u, "ok" if changed else "skip", took, None)
                if changed:
                    _inc_saved(1)
                return

        except Exception as e:
            # 失敗時フォールバック（Tavily raw）
            if tv:
                try:
                    raw = None
                    if hasattr(tv, "extract"):
                        raw = tv.extract(u).get("content")  # type: ignore[attr-defined]
                    if not raw:
                        r = tv.search(
                            u,
                            search_depth="basic",
                            max_results=1,
                            include_answer=False,
                            include_raw_content=True,
                        )
                        raw = (r.get("results") or [{}])[0].get("raw_content")
                    if raw:
                        with conn() as c:
                            row = extract_from_text(u, raw)
                            changed = upsert_page(c, row)
                            log_fetch(
                                c, u, "ok" if changed else "skip", 0, "fallback: raw"
                            )
                            if changed:
                                _inc_saved(1)
                        return
                except Exception as e2:
                    with conn() as c:
                        log_fetch(c, u, "ng", 0, f"fallback error: {e2}")
                        return
            # フォールバック不可なら ng
            with conn() as c:
                log_fetch(c, u, "ng", 0, str(e))
            return


def crawl() -> None:
    ensure_schema()
    sources = load_seeds()

    deadline = time.time() + TIME_BUDGET_SEC
    global _saved_count
    _saved_count = 0
    per_domain: dict[str, int] = {}

    with conn() as c_main:
        for src in sources:
            if time.time() > deadline:
                break

            list_url: str = src["url"]
            include = [re.compile(p) for p in src.get("include", [])]
            exclude = [re.compile(p) for p in src.get("exclude", [])]
            max_new = int(src.get("max_new", 20))
            use_discovery = src.get("discover") == "tavily" or (
                src.get("discover") is None and USE_TAVILY_DISCOVERY
            )

            # 一覧ページ：条件付きGET
            cur = c_main.cursor()
            cur.execute(
                "select etag, last_modified from public.http_cache where url=%s",
                (list_url,),
            )
            etag, lm = cur.fetchone() or (None, None)

            html = None
            try:
                html, new_etag, new_lm, ctype, status, took = conditional_fetch(
                    list_url, etag, lm
                )
                upsert_http_meta(c_main, list_url, new_etag, new_lm, status)
                log_fetch(
                    c_main,
                    list_url,
                    "list" if html is not None else "304",
                    took,
                    f"ctype={ctype or '(none)'}",
                )
            except Exception as e:
                log_fetch(c_main, list_url, "ng", 0, f"list error: {e}")

            # 候補URL収集：一覧の a[href] → include/exclude → doc-only
            candidates: List[str] = []
            if html and (not ctype or ctype.lower() in DOC_TYPES):
                links = extract_links(list_url, html)

                def path_ok(u: str) -> bool:
                    if include and not any(p.search(u) for p in include):
                        return False
                    if exclude and any(p.search(u) for p in exclude):
                        return False
                    return True

                candidates = [
                    u for u in links if allowed(u) and is_document_url(u) and path_ok(u)
                ]

            # Tavily discoveryで候補を補強
            if use_discovery:
                found = discover_with_tavily(src)
                candidates.extend(found)

            # 重複除外
            seen = set()
            uniq: List[str] = []
            for u in candidates:
                if u not in seen:
                    seen.add(u)
                    uniq.append(u)
            candidates = uniq

            # ドメインごとの上限を事前にあてがう
            filtered: List[str] = []
            for u in candidates:
                host = urlsplit(u).netloc
                cnt = per_domain.get(host, 0)
                if cnt < MAX_PER_DOMAIN:
                    filtered.append(u)
                    per_domain[host] = cnt + 1
                if len(filtered) >= max_new:
                    break

            # 最終リストをログ（何件拾えたか可視化）
            log_fetch(c_main, list_url, "list", 0, f"candidates={len(filtered)}")

            # 並列で詳細ページを処理
            if not filtered:
                continue
            with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
                futures = [ex.submit(process_detail, u, deadline) for u in filtered]
                for _ in as_completed(futures):
                    # 期限超過 or 件数到達なら残りをキャンセル（次のsourceへ）
                    if time.time() > deadline or _saved_count >= MAX_PAGES_PER_RUN:
                        for f in futures:
                            f.cancel()
                        break

            if time.time() > deadline or _saved_count >= MAX_PAGES_PER_RUN:
                break


if __name__ == "__main__":
    crawl()
