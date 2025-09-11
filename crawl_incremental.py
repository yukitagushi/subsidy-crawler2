# crawl_incremental.py — 増分クロール（ETag/Last-Modified）＋文書URL/Content-Type判定
# 依存: lib/http_client.py, lib/db.py, lib/extractors.py, seeds.yaml
# 環境変数:
#   TIME_BUDGET_SEC (default 240)     … この実行で使える最大秒数
#   MAX_PAGES_PER_RUN (default 60)    … 1回の実行で詳細ページを保存する最大件数
#   MAX_PER_DOMAIN (default 25)       … ドメインごとの保存上限（1回の実行）
#
# 使い方（GitHub Actions等）:
#   python crawl_incremental.py

from __future__ import annotations
import os
import re
import time
import yaml
from typing import Iterable, List, Set
from urllib.parse import urlsplit, urljoin

from bs4 import BeautifulSoup

from lib.http_client import conditional_fetch, head_ok  # returns (html, etag, last_mod, ctype, status, took_ms)
from lib.extractors import extract_from_html
from lib.db import conn, upsert_http_meta, upsert_page, log_fetch, ensure_schema

# -------- 設定（環境変数で上書き可） --------
TIME_BUDGET_SEC = int(os.getenv("TIME_BUDGET_SEC", "240"))
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN", "60"))
MAX_PER_DOMAIN = int(os.getenv("MAX_PER_DOMAIN", "25"))

# 受け入れる Content-Type（本文とみなす）
DOC_TYPES: Set[str] = {"text/html", "application/xhtml+xml", "application/pdf"}

# アセットURLを除外（script/css/画像/フォント/マップ等）
ASSET_RE = re.compile(
    r'\.(js|mjs|css|png|jpe?g|gif|svg|ico|json|map|woff2?|ttf|eot|mp4|webm)($|\?)',
    re.IGNORECASE,
)

ALLOWED_HOSTS: Set[str] = set()


def allowed(u: str) -> bool:
    """seeds.yaml の allowed_hosts に基づき許可ドメインのみ通す"""
    try:
        host = urlsplit(u).netloc
    except Exception:
        return False
    return any(host == d or host.endswith(d) for d in ALLOWED_HOSTS)


def is_document_url(u: str) -> bool:
    """明らかなアセットURL（.js 等）を除外"""
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
    """
    <a href="..."> だけを対象に抽出し、絶対URL化。
    script/linkタグやJSコード中のURLは拾わない。
    """
    out: List[str] = []
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        u = urljoin(base_url, href)
        out.append(u)
    # 重複を除外し順序を維持
    seen = set()
    uniq = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def crawl() -> None:
    """増分クロール本体。ETag/Last-Modified を使い、304 を活用。"""
    ensure_schema()  # 初回や新DBでも落ちないようにスキーマ適用

    sources = load_seeds()
    t_end = time.time() + TIME_BUDGET_SEC
    total_saved = 0
    saved_per_domain: dict[str, int] = {}

    with conn() as c:
        for src in sources:
            if time.time() > t_end:
                break

            list_url: str = src["url"]
            include = [re.compile(p) for p in src.get("include", [])]
            exclude = [re.compile(p) for p in src.get("exclude", [])]
            max_new = int(src.get("max_new", 20))

            # 一覧ページ HEAD（簡易死活）
            if not head_ok(list_url):
                log_fetch(c, list_url, "skip", 0, "HEAD failed")
                continue

            # 既存のETag/Last-Modified
            cur = c.cursor()
            cur.execute("select etag, last_modified from http_cache where url=%s", (list_url,))
            row = cur.fetchone()
            etag, lm = (row or (None, None))

            try:
                html, new_etag, new_lm, ctype, status, took = conditional_fetch(list_url, etag, lm)
                upsert_http_meta(c, list_url, new_etag, new_lm, status)
                log_fetch(c, list_url, "304" if html is None else "ok", took, None)
            except Exception as e:
                log_fetch(c, list_url, "ng", 0, str(e))
                continue

            # 一覧に変化なし or 非ドキュメント
            if html is None or (ctype and ctype.lower() not in DOC_TYPES):
                continue

            # 一覧から詳細候補を抽出→フィルタ
            links = extract_links(list_url, html)

            def path_ok(u: str) -> bool:
                if include and not any(p.search(u) for p in include):
                    return False
                if exclude and any(p.search(u) for p in exclude):
                    return False
                return True

            candidates = [u for u in links if allowed(u) and is_document_url(u) and path_ok(u)][:max_new]

            for u in candidates:
                if time.time() > t_end or total_saved >= MAX_PAGES_PER_RUN:
                    break
                host = urlsplit(u).netloc
                saved_per_domain[host] = saved_per_domain.get(host, 0) + 1
                if saved_per_domain[host] > MAX_PER_DOMAIN:
                    continue

                # 既存のETag/Last-Modified
                cur.execute("select etag, last_modified from http_cache where url=%s", (u,))
                row = cur.fetchone()
                petag, plm = (row or (None, None))

                try:
                    html, new_etag, new_lm, ctype, status, took = conditional_fetch(u, petag, plm)
                    upsert_http_meta(c, u, new_etag, new_lm, status)

                    # 304 or 非文書は除外
                    if html is None:
                        log_fetch(c, u, "304", took, None)
                        continue
                    if ctype and ctype.lower() not in DOC_TYPES:
                        log_fetch(c, u, "skip", took, f"ctype={ctype}")
                        continue

                    row_dict = extract_from_html(u, html)
                    changed = upsert_page(c, row_dict)
                    log_fetch(c, u, "ok" if changed else "skip", took, None)
                    if changed:
                        total_saved += 1
                except Exception as e:
                    log_fetch(c, u, "ng", 0, str(e))
                    continue


if __name__ == "__main__":
    crawl()