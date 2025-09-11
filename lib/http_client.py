import os, time, requests
from urllib.parse import urlsplit
from requests.adapters import HTTPAdapter, Retry

CONNECT = int(os.getenv("CONNECT_TIMEOUT","6"))
READ    = int(os.getenv("READ_TIMEOUT","25"))
HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; SubsidyBot/1.0)"}

S = requests.Session()
R = Retry(total=2, connect=2, read=0, backoff_factor=0.8,
          status_forcelist=[429,500,502,503,504],
          allowed_methods={"GET","HEAD"})
A = HTTPAdapter(max_retries=R, pool_maxsize=32)
S.mount("https://", A); S.mount("http://", A)

def head_ok(u: str, ct=3, rt=5) -> bool:
    try:
        S.head(u, headers=HEADERS, timeout=(ct, rt))
        return True
    except Exception:
        return False

def conditional_fetch(u: str, etag: str | None, last_mod: str | None):
    """If-None-Match / If-Modified-Since を付けて取得。304なら (None, etag, last_mod) を返す。"""
    hdr = dict(HEADERS)
    if etag: hdr["If-None-Match"] = etag
    if last_mod: hdr["If-Modified-Since"] = last_mod
    t0 = time.time()
    r = S.get(u, headers=hdr, timeout=(CONNECT, READ))
    took_ms = int((time.time() - t0) * 1000)
    r.raise_for_status() if r.status_code != 304 else None
    new_etag = r.headers.get("ETag") or etag
    new_lm   = r.headers.get("Last-Modified") or last_mod
    return (None if r.status_code == 304 else r.text, new_etag, new_lm, r.status_code, took_ms)
    