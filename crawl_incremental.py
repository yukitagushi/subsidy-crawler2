import os, time, re, yaml
from urllib.parse import urlsplit, urljoin

from lib.http_client import head_ok, conditional_fetch
from lib.extractors import extract_from_html
from lib.db import conn, upsert_http_meta, upsert_page, log_fetch

ALLOWED = set()
TIME_BUDGET_SEC = int(os.getenv("TIME_BUDGET_SEC","240"))
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN","60"))
MAX_PER_DOMAIN = int(os.getenv("MAX_PER_DOMAIN","25"))

def allowed(u: str) -> bool:
    try: host = urlsplit(u).netloc
    except: return False
    return any(host == d or host.endswith(d) for d in ALLOWED)

def load_seeds(path="seeds.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    global ALLOWED
    ALLOWED = set(cfg.get("allowed_hosts", []))
    return cfg.get("sources", [])

def find_links(base_url: str, html: str) -> set[str]:
    # 超簡易リンク抽出（正規表現ベース）：a href="..."
    hrefs = set([m.group(1) for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I)])
    out = set()
    for h in hrefs:
        if h.startswith("#") or h.startswith("javascript:"): continue
        u = urljoin(base_url, h)
        if allowed(u): out.add(u)
    return out

def crawl():
    sources = load_seeds()
    t_end = time.time() + TIME_BUDGET_SEC
    total_saved = 0
    per_domain = {}

    with conn() as c:
        for src in sources:
            if time.time() > t_end: break
            url = src["url"]
            include = [re.compile(p) for p in src.get("include", [])]
            exclude = [re.compile(p) for p in src.get("exclude", [])]
            max_new = int(src.get("max_new", 20))

            # 一覧ページ HEAD/GET（条件付き）
            if not head_ok(url): 
                log_fetch(c, url, "skip", 0, "HEAD failed"); 
                continue

            # 既存ETag/LMの取得
            cur = c.cursor()
            cur.execute("select etag, last_modified from http_cache where url=%s", (url,))
            row = cur.fetchone()
            etag, lm = (row or (None, None))

            try:
                html, new_etag, new_lm, status, took = conditional_fetch(url, etag, lm)
                upsert_http_meta(c, url, new_etag, new_lm, status)
                log_fetch(c, url, "304" if html is None else "ok", took, None)
            except Exception as e:
                log_fetch(c, url, "ng", 0, str(e)); 
                continue

            if html is None:
                continue  # 一覧に変化なし → 次のソースへ

            # 一覧から候補抽出→フィルタ
            links = list(find_links(url, html))
            def ok(u):
                if include and not any(p.search(u) for p in include): return False
                if exclude and any(p.search(u) for p in exclude): return False
                return True
            cand = [u for u in links if ok(u)]
            cand = cand[:max_new]

            for u in cand:
                if time.time() > t_end or total_saved >= MAX_PAGES_PER_RUN:
                    break
                host = urlsplit(u).netloc
                per_domain[host] = per_domain.get(host, 0) + 1
                if per_domain[host] > MAX_PER_DOMAIN:
                    continue

                # 条件付きGET
                cur.execute("select etag, last_modified from http_cache where url=%s", (u,))
                row = cur.fetchone()
                petag, plm = (row or (None, None))

                try:
                    html, new_etag, new_lm, status, took = conditional_fetch(u, petag, plm)
                    upsert_http_meta(c, u, new_etag, new_lm, status)
                    if html is None:
                        log_fetch(c, u, "304", took, None)
                        continue
                    row = extract_from_html(u, html)
                    changed = upsert_page(c, row)
                    log_fetch(c, u, "ok" if changed else "skip", took, None)
                    if changed: total_saved += 1
                except Exception as e:
                    log_fetch(c, u, "ng", 0, str(e))
                    continue

if __name__ == "__main__":
    crawl()