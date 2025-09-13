# lanes/lane_search_openai.py
import os
from typing import List, Dict
from openai import OpenAI

from lib.db import conn, log_fetch, upsert_page
from lib.util import norm_ws, clip

API_KEY = os.getenv("OPENAI_API_KEY", "")
ALLOWED = [s.strip() for s in os.getenv("DR_ALLOWED_DOMAINS", "").split(",") if s.strip()]
MODEL   = os.getenv("DR_MODEL", "o4-mini-deep-research-2025-06-26")
TIMEOUT = int(os.getenv("DR_TIMEOUT_SEC", "40"))

def discover_and_extract(query: str = "補助金 公募 申請 2025", max_items: int = 20) -> List[Dict]:
    if not API_KEY or not ALLOWED:
        return []
    client = OpenAI(api_key=API_KEY)

    prompt = (
        "以下のドメインのみ対象に、最新の『補助金の公募・要領・申請』の一次情報ページを探し、"
        "各ページの『タイトル・2〜3行の要約・主要数値（補助率/上限/年度/締切）』を抽出して、"
        "出典URL（citation）とともに JSON 配列で返してください。"
    )
    tools = [{"type": "web_search", "web_search": {"allow": ALLOWED}}]

    try:
        r = client.responses.create(
            model=MODEL,
            input=[{"role": "user", "content": f"{prompt}\nQuery: {query}"}],
            tools=tools,
            temperature=0.2,
            timeout=TIMEOUT
        )
    except Exception as e:
        with conn() as c: log_fetch(c, "openai:deep_research", "ng", 0, f"api error: {e}")
        return []

    # モデルの JSON 出力をパース
    items: List[Dict] = []
    try:
        txt = getattr(r, "output", None)
        if txt:
            txt = r.output[0].content[0].text
        else:
            txt = r.choices[0].message.content
        import json, re
        m = re.search(r"\[[\s\S]*\]", txt)
        arr = json.loads(m.group(0)) if m else json.loads(txt)

        for obj in arr[:max_items]:
            url = (obj.get("source") or obj.get("url") or "").strip()
            title = norm_ws(obj.get("title") or "")
            summary = norm_ws(obj.get("summary") or "")
            rate = norm_ws(obj.get("subsidy_rate") or obj.get("rate") or "")
            cap  = norm_ws(obj.get("max_amount") or obj.get("cap") or "")
            fy   = norm_ws(obj.get("fiscal_year") or "")
            call = norm_ws(obj.get("call_no") or "")

            if not url:
                continue
            items.append({
                "url": url, "title": title or "(無題)", "summary": clip(summary, 800),
                "rate": rate or None, "cap": cap or None, "target": None, "cost_items": None,
                "deadline": None, "fiscal_year": fy or None, "call_no": call or None,
                "scheme_type": None, "period_from": None, "period_to": None
            })
    except Exception as e:
        with conn() as c: log_fetch(c, "openai:deep_research", "ng", 0, f"parse error: {e}")
        return []

    saved = 0
    with conn() as c:
        for it in items:
            try:
                changed = upsert_page(c, it)
                log_fetch(c, it["url"], "ok" if changed else "skip", 0, "dr")
                if changed: saved += 1
            except Exception as e:
                log_fetch(c, it["url"], "ng", 0, f"upsert error: {e}")

    with conn() as c:
        log_fetch(c, "openai:deep_research", "list", 0, f"candidates={len(items)}; saved={saved}")
    return items
