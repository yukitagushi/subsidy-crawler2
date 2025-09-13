import os, json, re
from typing import List, Dict
from openai import OpenAI

from lib.db import conn, log_fetch, upsert_page
from lib.util import norm_ws, clip

API_KEY = os.getenv("OPENAI_API_KEY", "")
ALLOWED = [s.strip() for s in os.getenv("DR_ALLOWED_DOMAINS", "").split(",") if s.strip()]
MODEL   = os.getenv("DR_MODEL", "o4-mini-deep-research-2025-06-26")
TIMEOUT = int(os.getenv("DR_TIMEOUT_SEC", "40"))
MAX_ITEMS = int(os.getenv("DR_MAX_ITEMS", "40"))

URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.I)

def _allowed(url: str) -> bool:
    try:
        host = re.sub(r"^https?://", "", url).split("/")[0].lower()
        return any(host == d or host.endswith(d) for d in ALLOWED)
    except Exception:
        return False

def _row_from_minimal(url: str) -> Dict:
    return {
        "url": url, "title": "(無題)", "summary": None,
        "rate": None, "cap": None, "target": None, "cost_items": None,
        "deadline": None, "fiscal_year": None, "call_no": None,
        "scheme_type": None, "period_from": None, "period_to": None
    }

def discover_and_extract(query: str, max_items: int = MAX_ITEMS) -> List[Dict]:
    if not API_KEY or not ALLOWED:
        return []

    client = OpenAI(api_key=API_KEY)

    sys = (
        "You are a research assistant that ONLY returns valid JSON. "
        "No prose, no markdown. Output MUST be a JSON object with key 'items' that is an array."
    )
    user = (
        "allowed_domains 内だけを対象に、最新の『補助金の公募・要領・申請』の一次情報ページを探し、"
        "各ページについて次の項目を抽出して JSON で返してください。\n"
        "  - url (string)\n  - title (string)\n  - summary (string)\n"
        "  - subsidy_rate (string)\n  - max_amount (string)\n  - fiscal_year (string)\n  - call_no (string)\n"
        f"最大件数: {max_items} 件。重複URLは除外。allowed_domains の外は含めない。\n"
        f"クエリ: {query}\nallowed_domains: {ALLOWED}"
    )
    tools = [{"type": "web_search", "web_search": {"allow": ALLOWED}}]

    try:
        r = client.responses.create(
            model=MODEL,
            input=[{"role": "system", "content": sys},
                   {"role": "user", "content": user}],
            tools=tools,
            temperature=0.2,
            response_format={"type": "json_object"},
            timeout=TIMEOUT
        )
    except Exception as e:
        with conn() as c: log_fetch(c, "openai:deep_research", "ng", 0, f"api error: {e}")
        return []

    try:
        content = None
        if hasattr(r, "output"):
            content = r.output[0].content[0].text
        elif getattr(r, "choices", None):
            content = r.choices[0].message.content
        if not content:
            raise ValueError("empty response")

        obj = json.loads(content)
        arr = obj.get("items") if isinstance(obj, dict) else obj
        if not isinstance(arr, list):
            arr = []
    except Exception as e:
        # JSON 失敗→本文から URL を救済
        text = content if isinstance(content, str) else ""
        urls = [u for u in URL_RE.findall(text) if _allowed(u)]
        urls = list(dict.fromkeys(urls))[:max_items]
        items = [_row_from_minimal(u) for u in urls]
        with conn() as c: log_fetch(c, "openai:deep_research", "list", 0, f"fallback_links={len(items)}; parse_err={e}")
        saved = 0
        with conn() as c:
            for it in items:
                try:
                    changed = upsert_page(c, it)
                    log_fetch(c, it["url"], "ok" if changed else "skip", 0, "dr-fallback")
                    if changed: saved += 1
                except Exception as ee:
                    log_fetch(c, it["url"], "ng", 0, f"upsert error: {ee}")
        return items

    # 正常系
    items: List[Dict] = []
    for o in arr[:max_items]:
        url = norm_ws(o.get("url") or o.get("source") or "")
        if not url or not _allowed(url):
            continue
        title   = norm_ws(o.get("title") or "")
        summary = norm_ws(o.get("summary") or "")
        rate = norm_ws(o.get("subsidy_rate") or o.get("rate") or "")
        cap  = norm_ws(o.get("max_amount")  or o.get("cap")  or "")
        fy   = norm_ws(o.get("fiscal_year") or "")
        call = norm_ws(o.get("call_no") or "")
        items.append({
            "url": url, "title": title or "(無題)", "summary": clip(summary, 800),
            "rate": rate or None, "cap": cap or None, "target": None, "cost_items": None,
            "deadline": None, "fiscal_year": fy or None, "call_no": call or None,
            "scheme_type": None, "period_from": None, "period_to": None
        })

    saved = 0
    with conn() as c:
        for it in items:
            try:
                changed = upsert_page(c, it)
                log_fetch(c, it["url"], "ok" if changed else "skip", 0, "dr")
                if changed: saved += 1
            except Exception as ee:
                log_fetch(c, it["url"], "ng", 0, f"upsert error: {ee}")
    with conn() as c:
        log_fetch(c, "openai:deep_research", "list", 0, f"candidates={len(items)}; saved={saved}")
    return items
