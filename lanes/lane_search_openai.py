# lanes/lane_search_openai.py
# OpenAI Deep Research API を使って、指定ドメイン内で候補URLと要点を取得
# 参考: Deep Research API ガイド/ツールのWeb検索ガイド（公式） [oai_citation:3‡OpenAI Platform](https://platform.openai.com/docs/guides/deep-research?utm_source=chatgpt.com)
import os, time
from typing import List, Dict
from openai import OpenAI

from lib.db import conn, log_fetch, upsert_page
from lib.util import norm_ws, clip

API_KEY = os.getenv("OPENAI_API_KEY", "")
ALLOWED = [s.strip() for s in os.getenv("DR_ALLOWED_DOMAINS", "").split(",") if s.strip()]
MODEL   = os.getenv("DR_MODEL", "o4-mini-deep-research-2025-06-26")  # 軽量推奨  [oai_citation:4‡apidog](https://apidog.com/blog/openai-deep-research-api/?utm_source=chatgpt.com)
TIMEOUT = int(os.getenv("DR_TIMEOUT_SEC", "40"))

def discover_and_extract(query: str = "補助金 公募 申請 2025", max_items: int = 20) -> List[Dict]:
    """
    returns: [{"url":..., "title":..., "summary":..., ...}, ...]
    """
    if not API_KEY or not ALLOWED:
        return []
    client = OpenAI(api_key=API_KEY)

    prompt = (
        "以下のドメインだけを対象に、最新の『補助金の公募・要領・申請』に関する一次情報ページを探し、"
        "各ページについて『タイトル・要約（2〜3行）・主要数値（補助率/上限/締切/年度）』を抽出してください。"
        "各項目には必ず出典URL（citation）を付け、JSON配列で返してください。"
    )
    # Deep Research の Web 検索ツールを許可し、allowed_domains を指定（公式ガイド相当） [oai_citation:5‡OpenAI Platform](https://platform.openai.com/docs/guides/tools-web-search?utm_source=chatgpt.com)
    tools = [{"type": "web_search", "web_search": {"allow": ALLOWED}}]

    try:
        # Responses API 相当の呼び出し（例：非同期/同期どちらでもOK。ここは同期で簡潔に）
        r = client.responses.create(
            model=MODEL,
            input=[{"role": "user", "content": f"{prompt}\nQuery: {query}"}],
            tools=tools,
            temperature=0.2,
            timeout=TIMEOUT
        )
    except Exception as e:
        # API レベルの失敗（ネットワーク等）
        with conn() as c: log_fetch(c, "openai:deep_research", "ng", 0, f"api error: {e}")
        return []

    # モデルの出力（JSON文字列）を想定してパース
    items: List[Dict] = []
    try:
        txt = r.output[0].content[0].text if hasattr(r, "output") else r.choices[0].message.content  # SDK差異の吸収
        # 出力はJSON配列を前提（Cookbookの推奨パターン） [oai_citation:6‡OpenAI Cookbook](https://cookbook.openai.com/examples/deep_research_api/introduction_to_deep_research_api?utm_source=chatgpt.com)
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

    # DBへ upsert（pages）＋ログ
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
