import os, time, requests
from urllib.parse import urlsplit
from requests.adapters import HTTPAdapter, Retry

# === 既定タイムアウト（ENV） ===
CONNECT = int(os.getenv("CONNECT_TIMEOUT", "12"))
READ    = int(os.getenv("READ_TIMEOUT", "45"))
HOST_READ = { "www.chusho.meti.go.jp": int(os.getenv("CHUSHO_READ_TIMEOUT", "75")) }

# === シリアルRun専用の「強制READ/CONNECTタイムアウト」 ===
SINGLE_FORCE_READ_TIMEOUT    = int(os.getenv("SINGLE_FORCE_READ_TIMEOUT", "0"))   # 0=無効
SINGLE_FORCE_CONNECT_TIMEOUT = int(os.getenv("SINGLE_FORCE_CONNECT_TIMEOUT", "0"))# 0=無効
SINGLE_MODE                  = os.getenv("SINGLE_BACKFILL_ONE", "0") == "1"

# === 共通ヘッダ（接続拒否を避けるため UA/Accept を堅めに） ===
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf;q=0.8,*/*;q=0.7",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

# === リトライ設定 ===
#   - シリアル時は connect/read ともに回数を増やし、バックオフも少し長め
#   - 通常時は従来どおり
if SINGLE_MODE:
    retry = Retry(
        total=5, connect=5, read=2,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"}
    )
else:
    retry = Retry(
        total=3, connect=3, read=0,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"}
    )

S = requests.Session()
A = HTTPAdapter(max_retries=retry, pool_maxsize=32)
S.mount("https://", A); S.mount("http://", A)

def conditional_fetch(u, etag, last_mod):
    """
    GETを実行して本文を返す。
    - etag/last_mod があれば If-None-Match/If-Modified-Since を付与
    - シリアル時は SINGLE_FORCE_CONNECT/READ が指定されていればそれを最優先
    - 次に HOST_READ、最後に既定 READ
    戻り値: (html or None, new_etag, new_last_mod, content_type, status_code, took_ms)
    """
    host = urlsplit(u).netloc

    # タイムアウト決定
    rt = SINGLE_FORCE_READ_TIMEOUT or HOST_READ.get(host, READ)
    ct = SINGLE_FORCE_CONNECT_TIMEOUT or CONNECT

    hdr = dict(HEADERS)
    if etag:     hdr["If-None-Match"] = etag
    if last_mod: hdr["If-Modified-Since"] = last_mod

    t0 = time.time()
    r  = S.get(u, headers=hdr, timeout=(ct, rt), allow_redirects=True)
    took = int((time.time()-t0)*1000)

    ctype = (r.headers.get("Content-Type") or "").split(";")[0].lower()

    if r.status_code == 304:
        return None, etag, last_mod, ctype, r.status_code, took

    r.raise_for_status()
    return r.text, r.headers.get("ETag") or etag, r.headers.get("Last-Modified") or last_mod, ctype, r.status_code, took
