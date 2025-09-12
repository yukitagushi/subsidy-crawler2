from flask import Flask, request, jsonify
import os, json, psycopg, re, unicodedata, time
from openai import OpenAI

app = Flask(__name__)
DSN = os.getenv("DATABASE_URL")
OPENAI_MODEL = os.getenv("OPENAI_MODEL","gpt-4o-mini")
client = OpenAI()

def _norm(s): 
    if not s: return ""
    return unicodedata.normalize("NFKC", str(s))

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
        order by last_fetched desc limit %s
        """, (q, limit))
    else:
        cur.execute("""
          select url,title,summary,rate,cap,target,cost_items,deadline,fiscal_year,call_no,scheme_type,
                 period_from,period_to,last_fetched
            from pages
        order by last_fetched desc limit %s
        """, (limit,))
    cols=[d.name for d in cur.description]
    return [dict(zip(cols,row)) for row in cur]

def _llm_score(it:dict, profile:dict)->tuple[float,list]:
    t=" ".join(_to_text(it.get(k)) for k in ("title","summary","target","cost_items","rate","cap"))
    prompt=("あなたは補助金マッチングの査定者。事業者プロファイルとの適合度を0〜100で採点し、"
            "根拠を2〜4点、JSONで返してください。\n"
            f"[事業者]\n{json.dumps(profile,ensure_ascii=False)}\n"
            f"[制度テキスト]\n{t[:4000]}\n"
            '出力: {"score": 数値, "reasons":["..."]}')
    score=50.0; reasons=[]
    try:
        r=client.chat.completions.create(model=OPENAI_MODEL,
             messages=[{"role":"user","content":prompt}], temperature=0.2, max_tokens=300)
        txt=r.choices[0].message.content.strip()
        m=re.search(r"\{[\s\S]*\}", txt)
        if m:
            obj=json.loads(m.group(0))
            score=float(obj.get("score",score))
            reasons=[_norm(x) for x in obj.get("reasons",[]) if x]
    except Exception as e:
        reasons=[f"llm error: {e}"]
    return score, reasons

@app.post("/")
def handler():
    d = request.get_json(force=True) or {}
    profile = {
      "所在地_都道府県": d.get("prefecture",""),
      "目的": d.get("goal",""),
      "対象経費カテゴリ": d.get("cost_categories",[])
    }
    q = d.get("query") or None
    items=[]
    with psycopg.connect(DSN, autocommit=True) as c, c.cursor() as cur:
        rows=_search(cur, q, limit=40)
        for r in rows:
            it=dict(r)
            sc,why=_llm_score(it, profile)
            it.update({"score": sc, "why": why})
            items.append(it)
    items.sort(key=lambda x:(-(x.get("score") or 0), _norm(x.get("title"))))
    return jsonify({"items":items, "kpi":{"elapsed_ms":0}})

# Cloud Functions (gen2) entry: app
