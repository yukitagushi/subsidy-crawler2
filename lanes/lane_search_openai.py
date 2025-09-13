# lanes/lane_search_openai.py
import os, json, re
from typing import List, Dict
from openai import OpenAI

from lib.db import conn, log_fetch, upsert_page
from lib.util import norm_ws, clip

API_KEY = os.getenv("OPENAI_API_KEY", "")
ALLOWED = [s.strip() for s in os.getenv("DR_ALLOWED_DOMAINS", "").split(",") if s.strip()]
MODEL   = os.getenv("DR_MODEL", "o4-mini-deep-research-2025-06-26")
TIMEOUT = int(os.getenv("DR_TIMEOUT_SEC", "40"))

def discover_and_extract(query: str = "補助金 公募 申請 2025", max_items: int = 40) -> List[Dict]:
    """
    OpenAI Deep Research で allowed_domains 内だけを探索し、URLと要点を抽出。
    JSON出力を強制して堅牢にパース。抽出できたものは pages に upsert してから返す。
    """
    if not API_KEY or not ALLOWED:
        return []
    client = OpenAI(api_key=API_KEY)

    sys = (
        "You are a research assistant that ONLY returns valid JSON. "
        "No prose, no markdown. Output MUST be a JSON array of objects."
    )
    user = (
        "対象ドメイン（allowed_domains）内だけで、最新の『補助金の公募・要領・申請』に関する一次情報ページを探し、"
        "各ページについて次の項目を抽出してください。各オブジェクトは必ず下記キーを持つこと:\n"
        "  - url (string; 出典URL)\n"
        "  - title (string; タイトル)\n"
        "  - summary (string; 2〜3行の要約)\n"
        "  - subsidy_rate (string; 補助率があれば)\n"
        "  - max_amount (string; 上限額があれば)\n"
        "  - fiscal_year (string; 年度があれば)\n"
        "  - call_no (string; 公募回があれば)\n"
        f"件数は最大 {max_items} 件。重複や同一URLは除外。allowed_domains の外は検索しない。\n"
        f"クエリ: {query}\n"
        f"allowed_domains: {ALLOWED}\n"
        "必ず JSON 配列のみを返してください。"
    )
    tools = [{"type": "web_search", "web_search": {"allow": ALLOWED}}]

    try:
        r = client.responses.create(
            model=MODEL,
            input=[{"role": "system", "content": sys},
                   {"role": "user", "content": user}],
            tools=tools,
            temperature=0.2,
            response_format={"type":"json_object"},   # ← JSONを強制
            timeout=TIMEOUT
        )
    except Exception as e:
        with conn() as c: log_fetch(c, "openai:deep_research", "ng", 0, f"api error: {e}")
        return []

    # 出力取り出し
    try:
        # SDKの出力形が将来変わっても拾えるように防御的に書く
        if hasattr(r, "output"):
            content = r.output[0].content[0].text
        else:
            content = r.choices[0].message.content
        # JSON object で返るため、配列を取り出す（{"items":[...]} or 直接配列）
        obj = json.loads(content)
        arr = obj.get("items") if isinstance(obj, dict) else obj
        if not isinstance(arr, list):
            arr = []
    except Exception as e:
        with conn() as c: log_fetch(c, "openai:deep_research", "ng", 0, f"parse error: {e}")
        return []

    # 正規化＆DB保存
    items: List[Dict] = []
    saved = 0
    with conn() as c:
        for o in arr:
            try:
                url = norm_ws(o.get("url") or o.get("source") or "")
                if not url: 
                    continue
                title   = norm_ws(o.get("title") or "")
                summary = norm_ws(o.get("summary") or "")
                rate = norm_ws(o.get("subsidy_rate") or o.get("rate") or "")
                cap  = norm_ws(o.get("max_amount") or o.get("cap") or "")
                fy   = norm_ws(o.get("fiscal_year") or "")
                call = norm_ws(o.get("call_no") or "")

                row = {
                    "url": url, "title": title or "(無題)", "summary": clip(summary, 800),
                    "rate": rate or None, "cap": cap or None, "target": None, "cost_items": None,
                    "deadline": None, "fiscal_year": fy or None, "call_no": call or None,
                    "scheme_type": None, "period_from": None, "period_to": None
                }
                changed = upsert_page(c, row)
                log_fetch(c, url, "ok" if changed else "skip", 0, "dr")
                if changed: saved += 1
                items.append(row)
            except Exception as e:
                log_fetch(c, o.get("url",""), "ng", 0, f"upsert error: {e}")

    with conn() as c:
        log_fetch(c, "openai:deep_research", "list", 0, f"candidates={len(items)}; saved={saved}")
    return items
