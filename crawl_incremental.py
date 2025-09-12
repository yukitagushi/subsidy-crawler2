# crawl_incremental.py — 増分クロール（ETag/Last-Modified）＋文書URL/Content-Type判定＋フォールバック
from __future__ import annotations

import os
import re
import time
import yaml
from typing import List, Set
from urllib.parse import urlsplit, urljoin

from bs4 import BeautifulSoup

from lib.http_client import conditional_fetch
from lib.extractors import extract_from_html, extract_from_text
from lib.db import conn, upsert_http_meta, upsert_page, log_fetch, ensure_schema

# ---- Tavily raw fallback（キーが無ければ自動で無効） ----
try:
    from tavily import TavilyClient  # pip: tavily-python
except Exception:
    TavilyClient = None

TAVILY_KEY = os.getenv("TAVILY_API_KEY")
tv = TavilyClient(api_key=TAVILY_KEY) if (TAVILY_KEY and TavilyClient) else None

# -------- 設定（ENVで上書き可） --------
TIME_BUDGET_SEC   = int(os.getenv("TIME_BUDGET_SEC", "240"))
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN", "60"))
MAX_PER_DOMAIN    = int(os.getenv("MAX_PER_DOMAIN", "25"))

DOC_TYPES: Set[str] = {"text/html", "application/xhtml+xml", "application/pdf"}
ASSET_RE = re.compile(
    r'\.(js|mjs|css|png|jpe?g|gif|svg|ico|json|map|woff2?|ttf|eot|mp4|webm)($|\?)',
    re.I
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
    """a[href] だけを抽出。script/link等は拾わない。"""
    soup = BeautifulSoup(html, "html.parser")
    out: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        out.append(urljoin(base_url, href))
    # 重複排除（順序維持）
    seen = set()
    uniq: List[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def crawl() -> None:
    ensure_schema()
    sources = load_seeds()
    t_end = time.time() + TIME_BUDGET_SEC
    total_saved = 0
    per_domain: dict[str, int] = {}

    with conn() as c:
        for src in sources:
            if time.time() > t_end:
                break

            list_url = src["url"]
            include = [re.compile(p) for p in src.get("include", [])]
            exclude = [re.compile(p) for p in src.get("exclude", [])]
            max_new = int(src.get("max_new", 20))

            # 既存 ETag/LM
            cur = c.cursor()
            cur.execute(
                "select etag, last_modified from http_cache where url=%s", (list_url,)
            )
            etag, lm = cur.fetchone() or (None, None)

            # 一覧：条件付き GET
            try:
                html, new_etag, new_lm, ctype, status, took = conditional_fetch(
                    list_url, etag, lm
                )
                upsert_http_meta(c, list_url, new_etag, new_lm, status)
                log_fetch(c, list_url, "304" if html is None else "ok", took, None)
            except Exception as e:
                log_fetch(c, list_url, "ng", 0, str(e))
                continue

            if html is None or (ctype and ctype.lower() not in DOC_TYPES):
                continue

            # 詳細候補を抽出→フィルタ
            links = extract_links(list_url, html)

            def path_ok(u: str) -> bool:
                if include and not any(p.search(u) for p in include):
                    return False
                if exclude and any(p.search(u) for p in exclude):
                    return False
                return True

            candidates = [
                u for u in links if allowed(u) and is_document_url(u) and path_ok(u)
            ][:max_new]

            for u in candidates:
                if time.time() > t_end or total_saved >= MAX_PAGES_PER_RUN:
                    break

                host = urlsplit(u).netloc
                per_domain[host] = per_domain.get(host, 0) + 1
                if per_domain[host] > MAX_PER_DOMAIN:
                    continue

                # 既存 ETag/LM
                cur.execute(
                    "select etag, last_modified from http_cache where url=%s", (u,)
                )
                petag, plm = cur.fetchone() or (None, None)

                try:
                    html, new_etag, new_lm, ctype, status, took = conditional_fetch(
                        u, petag, plm
                    )
                    upsert_http_meta(c, u, new_etag, new_lm, status)

                    if html is None:
                        log_fetch(c, u, "304", took, None)
                        continue
                    if ctype and ctype.lower() not in DOC_TYPES:
                        log_fetch(c, u, "skip", took, f"ctype={ctype}")
                        continue

                    row = extract_from_html(u, html)
                    changed = upsert_page(c, row)
                    log_fetch(c, u, "ok" if changed else "skip", took, None)
                    if changed:
                        total_saved += 1

                except Exception as e:
                    # ---- フォールバック：Tavily raw_content ----
                    if tv:
                        try:
                            raw = None
                            # extract API が使えれば優先
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
                                row = extract_from_text(u, raw)
                                changed = upsert_page(c, row)
                                log_fetch(
                                    c, u, "ok" if changed else "skip", 0, "fallback: raw"
                                )
                                if changed:
                                    total_saved += 1
                                continue
                        except Exception as e2:
                            log_fetch(c, u, "ng", 0, f"fallback error: {e2}")
                            continue

                    # フォールバック不可なら ng
                    log_fetch(c, u, "ng", 0, str(e))
                    continue


if __name__ == "__main__":
    crawl()