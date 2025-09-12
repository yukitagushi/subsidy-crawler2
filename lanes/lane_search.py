import os, time, requests
from urllib.parse import quote_plus
from lib.db import conn, log_fetch

BING_KEY = os.getenv("BING_SEARCH_KEY")
BING_EP  = os.getenv("BING_SEARCH_ENDPOINT", "https://api.bing.microsoft.com/v7.0/search")

DEFAULT_QUERIES = [
    "公募 補助金 申請 2025",
    "募集 補助金 2025",
    "助成金 申請 2025"
]

ALLOWED_DOMAINS = [
    "www.chusho.meti.go.jp", "chusho.meti.go.jp",
    "www.meti.go.jp", "meti.go.jp",
    "www.jgrants-portal.go.jp", "jgrants-portal.go.jp",
]

def _bing(q:str, count:int=20)->list[str]:
    if not BING_KEY: return []
    headers={"Ocp-Apim-Subscription-Key": BING_KEY}
    params={"q": q, "count": count, "mkt": "ja-JP"}
    r=requests.get(BING_EP, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    js=r.json()
    out=[]
    for w in (js.get("webPages") or {}).get("value", []):
        url=w.get("url") or ""
        if url: out.append(url)
    return out

def discover(max_results:int=40) -> list[str]:
    """ALLOWED_DOMAINS に限定した site: クエリで候補を取得"""
    results=[]
    for domain in ALLOWED_DOMAINS:
        for q in DEFAULT_QUERIES:
            query = f"site:{domain} {q}"
            try:
                urls=_bing(query, count=10)
                results.extend(urls)
            except Exception:
                continue
    # 重複排除
    seen=set(); uniq=[]
    for u in results:
        if u not in seen:
            seen.add(u); uniq.append(u)
    with conn() as c:
        log_fetch(c, "bing:discovery", "list", 0, f"candidates={len(uniq)}")
    return uniq[:max_results]
