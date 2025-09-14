# lanes/lane_search_openai.py
# Deep Research（Responses API）を使って allowed_domains 内のURLから
# 1) 候補列挙（既存 discover_and_extract）
# 2) 任意URLの本文テキスト抽出（dr_fetch_text）←今回追加
#
# ENV:
#   OPENAI_API_KEY
#   DR_ALLOWED_DOMAINS (カンマ区切り)
#   DR_MODEL (既定: o4-mini-deep-research-2025-06-26)
#   DR_TIMEOUT_SEC (既定: 40)
#   DR_MAX_ITEMS (既定: 40)

import os, json, re
from typing import List, Dict, Optional
from urllib.parse import urlparse
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
        host = urlparse(url).netloc.lower()
        return any(host == d or host.endswith(d) for d in ALLOWED)
    except Exception:
        return False

# ---------- 1) 候補列挙（既存） ----------
def discover_and_extract(query: str, max_items: int = MAX_ITEMS) -> List[Dict]:
    if not API_KEY or not ALLOWED:
        return []
    client = OpenAI(api_key=API_KEY)

    sys = (
        "You are a research assistant that ONLY returns valid JSON. "
        "No prose, no markdown. Output MUST be a JSON object with key 'items' that is an array."
    )
    user = (
        "allowed_domains 内だけで、最新の『補助金の公募・要領・申請』の一次情報ページを探し、"
        "各ページの {url,title,summary,subsidy_rate,max_amount,fiscal_year,call_no} を抽出して返してください。"
        f"最大 {max_items} 件。重複URLは除外。allowed_domains の外は含めない。\n"
        f"allowed_domains: {ALLOWED}\n"
        "クエリ: " + query
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
        content = (r.output[0].content[0].text
                   if hasattr(r, "output") else r.choices[0].message.content)
        obj = json.loads(content) if content else {}
        arr = obj.get("items") if isinstance(obj, dict) else (obj if isinstance(obj, list) else [])
    except Exception as e:
        # JSON失敗→本文からURL救済
        text = content or ""
        urls = [u for u in URL_RE.findall(text) if _allowed(u)]
        urls = list(dict.fromkeys(urls))[:max_items]
        items = [{"url": u, "title": "(無題)", "summary": None} for u in urls]
        with conn() as c: log_fetch(c, "openai:deep_research", "list", 0, f"fallback_links={len(items)}; parse_err={e}")
        # upsert & return
        saved = 0
        with conn() as c:
            for it in items:
                try:
                    it_full = {
                        "url": it["url"], "title": "(無題)", "summary": None,
                        "rate": None, "cap": None, "target": None, "cost_items": None,
                        "deadline": None, "fiscal_year": None, "call_no": None,
                        "scheme_type": None, "period_from": None, "period_to": None
                    }
                    changed = upsert_page(c, it_full)
                    log_fetch(c, it["url"], "ok" if changed else "skip", 0, "dr-fallback")
                    if changed: saved += 1
                except Exception as ee:
                    log_fetch(c, it["url"], "ng", 0, f"upsert error: {ee}")
        return items

    # 正常時：正規化して返す
    items: List[Dict] = []
    for o in (arr or [])[:max_items]:
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

# ---------- 2) 任意URLの本文テキスト抽出（今回追加） ----------
def dr_fetch_text(url: str, max_chars: int = 6000) -> Optional[str]:
    """
    Deep Researchに「このURLを読んで本文プレーンテキストだけ返して」と依頼。
    返りは JSON {text: "..."} を想定。allowed_domains に含まれていないURLは None。
    """
    if not API_KEY or not _allowed(url):
        return None

    client = OpenAI(api_key=API_KEY)

    sys = ("You extract readable main text content from a given URL. "
           "Return ONLY JSON object: {\"text\": \"...\"}. No prose, no markdown.")
    user = (f"URL: {url}\n"
            f"Allowed domains: {ALLOWED}\n"
            "Fetch the page and return its readable main text in Japanese if possible. "
            f"Max characters: {max_chars}. Remove navigation, footer, menu, scripts.")

    tools = [{"type": "web_search", "web_search": {"allow": ALLOWED}}]

    try:
        r = client.responses.create(
            model=MODEL,
            input=[{"role": "system", "content": sys},
                   {"role": "user", "content": user}],
            tools=tools,
            temperature=0.0,
            response_format={"type": "json_object"},
            timeout=TIMEOUT
        )
        content = (r.output[0].content[0].text
                   if hasattr(r, "output") else r.choices[0].message.content)
        if not content:
            return None
        obj = json.loads(content)
        txt = obj.get("text") if isinstance(obj, dict) else None
        if isinstance(txt, str) and txt.strip():
            return txt[:max_chars]
    except Exception as e:
        with conn() as c: log_fetch(c, url, "ng", 0, f"dr-fetch error: {e}")
    return None
