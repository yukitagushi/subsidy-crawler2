import os, json, time, psycopg, re, unicodedata
from typing import Dict, Any, List
from openai import OpenAI
client = OpenAI()

DSN = os.getenv("DATABASE_URL")
OPENAI_MODEL = os.getenv("OPENAI_MODEL","gpt-4o-mini")

def _norm(s): 
    if not s: return ""
    return unicodedata.normalize("NFKC", s)

def _to_text(x):
    if x is None: return ""
    if isinstance(x,(list,tuple,set)): return "、".join(map(_to_text,x))
    if isinstance(x,dict): return "、".join(f"{k}:{_to_text(v)}" for k,v in x.items())
    return str(x)

def _search(cur, q:str|None, limit:int=40)->list[dict]:
    if q:
        cur.execute("""
          select url,title,summary,rate,cap,target,cost_items,deadline,fiscal_year,call_no,scheme_type,
                 period_from,period_to,last_fetched
          from pages
          where tokens @@ plainto_tsquery('simple', %s)
          order by last_fetched desc
          limit %s
        """, (q, limit))
    else:
        cur.execute("""
          select url,title,summary,rate,cap,target,cost_items,deadline,fiscal_year,call_no,scheme_type,
                 period_from,period_to,last_fetched
          from pages order by last_fetched desc limit %s
        """, (limit,))
    cols=[d.name for d in cur.description]
    return [dict(zip(cols,row)) for row in cur]

def _llm_score(it:dict, profile:dict)->tuple[float,list]:
    t=" ".join(_to_text(it.get(k)) for k in ("title","summary","target","cost_items","rate","cap"))
    prompt = ("あなたは補助金マッチングの査定者です。\n"
      "以下の事業者プロファイルと制度概要テキストの適合度を0〜100点で出し、"
      "根拠箇条書きを2〜4点、JSONで返してください。\n\n"
      f"[事業者]\n{json.dumps(profile,ensure_ascii=False)}\n\n"
      f"[制度テキスト]\n{t[:4000]}\n\n"
      "出力: {\"score\": 数値, \"reasons\":[\"...\"] }")
    score=50.0; reasons=[]
    try:
        r=client.chat.completions.create(model=OPENAI_MODEL,
            messages=[{"role":"user","content":prompt}],temperature=0.2,max_tokens=300)
        txt=r.choices[0].message.content.strip()
        m=re.search(r"\{[\s\S]*\}", txt)
        if m:
            obj=json.loads(m.group(0)); score=float(obj.get("score",score))
            reasons=[_norm(x) for x in obj.get("reasons",[]) if x]
    except Exception as e:
        reasons=[f"llm error: {e}"]
    return score, reasons

def recommend_from_db(profile:dict, query:str|None=None, limit:int=40)->dict:
    t0=time.time(); items=[]
    with psycopg.connect(DSN, autocommit=True) as c, c.cursor() as cur:
        rows=_search(cur, query, limit=limit)
        for r in rows:
            it=dict(r); sc,why=_llm_score(it, profile)
            it.update({"score": sc, "why": why, "why_table": [
                {"項目":"所在地","入力":_to_text(profile.get("所在地_都道府県")),"制度側":_to_text(it.get("target")),"評価":"-"},
                {"項目":"目的","入力":_to_text(profile.get("目的")),"制度側":_to_text(it.get("summary")),"評価":"-"},
                {"項目":"対象経費","入力":_to_text(profile.get("対象経費カテゴリ")),"制度側":_to_text(it.get("cost_items")),"評価":"-"},
            ], "last_checked_at": it.pop("last_fetched", None)})
            items.append(it)
    items.sort(key=lambda x: (-(x.get("score") or 0), _norm(x.get("title"))))
    return {"items":items, "excluded":[], "kpi":{"elapsed_ms": int((time.time()-t0)*1000), "seeds": len(items)}}