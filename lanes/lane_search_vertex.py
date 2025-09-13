import os, requests, re
from lib.db import conn, log_fetch

# --- Secrets を防御的に正規化（改行/空白を除去） ---
def _clean(s: str | None) -> str:
    if not s:
        return ""
    return "".join(str(s).split())  # 改行・タブ・全角空白も含めて削除

API_KEY = _clean(os.getenv("GOOGLE_API_KEY"))
SERVING_CONFIG = _clean(os.getenv("VERTEX_SERVING_CONFIG"))  # projects/.../servingConfigs/default_search

# 形式チェック（誤っていたらログだけ残して終了）
_SC_PAT = re.compile(
    r"^projects\/[^\/]+\/locations\/(global|us|eu)\/collections\/default_collection\/"
    r"(engines|dataStores)\/[^\/]+\/servingConfigs\/(default_search|default_serving_config)$"
)

def discover(query="公募 補助金 申請 2025", page_size=25, max_pages=2) -> list[str]:
    """
    Vertex AI Search searchLite (APIキー) で候補URLを取得（公開Webのみ）。
    """
    if not API_KEY or not SERVING_CONFIG:
        return []

    if not _SC_PAT.match(SERVING_CONFIG):
        try:
            with conn() as c:
                log_fetch(c, "vertex:discovery", "ng", 0, f"malformed servingConfig: {SERVING_CONFIG}")
        finally:
            return []

    urls: list[str] = []
    page_token = None

    for _ in range(max_pages):
        body = {"servingConfig": SERVING_CONFIG, "query": query, "pageSize": page_size}
        if page_token:
            body["pageToken"] = page_token

        r = requests.post(
            f"https://discoveryengine.googleapis.com/v1/{SERVING_CONFIG}:searchLite",
            headers={"x-goog-api-key": API_KEY, "Content-Type": "application/json"},
            json=body,
            timeout=20,
        )
        r.raise_for_status()
        js = r.json()

        for res in (js.get("results") or []):
            doc = res.get("document") or {}
            link = (doc.get("derivedStructData") or {}).get("link") \
                   or (doc.get("structData") or {}).get("link") \
                   or doc.get("id")
            if link:
                urls.append(link)

        page_token = js.get("nextPageToken")
        if not page_token:
            break

    # 重複除去
    seen, uniq = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)

    # 収集件数を fetch_log に記録
    with conn() as c:
        log_fetch(c, "vertex:discovery", "list", 0, f"candidates={len(uniq)}")

    return uniq
