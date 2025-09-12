import os, time, requests
from urllib.parse import urlsplit
from requests.adapters import HTTPAdapter, Retry

CONNECT = int(os.getenv("CONNECT_TIMEOUT", "10"))
READ    = int(os.getenv("READ_TIMEOUT", "35"))
HOST_READ = { "www.chusho.meti.go.jp": int(os.getenv("CHUSHO_READ_TIMEOUT", "45")) }

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

S = requests.Session()
R = Retry(total=3, connect=3, read=0, backoff_factor=1.2,
          status_forcelist=[429,500,502,503,504], allowed_methods={"GET"})
A = HTTPAdapter(max_retries=R, pool_maxsize=32)
S.mount("https://", A); S.mount("http://", A)

def conditional_fetch(u: str, etag: str | None, last_mod: str | None):
    # ★ 304回避の一時スイッチ（crawl.ymlで FORCE_REFRESH=1 のとき）
    if os.getenv("FORCE_REFRESH", "0") == "1":
        etag = None; last_mod = None

    host = urlsplit(u).netloc
    rt = HOST_READ.get(host, READ)
    hdr = dict(HEADERS)
    if etag:     hdr["If-None-Match"] = etag
    if last_mod: hdr["If-Modified-Since"] = last_mod

    t0 = time.time()
    r = S.get(u, headers=hdr, timeout=(CONNECT, rt))
    took_ms = int((time.time()-t0)*1000)
    ctype = (r.headers.get("Content-Type") or "").split(";")[0].lower()

    if r.status_code == 304:
        return None, etag, last_mod, ctype, r.status_code, took_ms

    r.raise_for_status()
    new_etag = r.headers.get("ETag") or etag
    new_lm   = r.headers.get("Last-Modified") or last_mod
    return r.text, new_etag, new_lm, ctype, r.status_code, took_ms