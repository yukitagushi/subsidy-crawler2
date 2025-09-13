from flask import Flask, request, jsonify
import os, json, psycopg, re, unicodedata
from openai import OpenAI

app=Flask(__name__)
DSN=os.getenv("DATABASE_URL")
OPENAI_MODEL=os.getenv("OPENAI_MODEL","gpt-4o-mini")
client=OpenAI()

def _norm(s): return unicodedata.normalize("NFKC", s or "")
def _to_text(x):
    if x is None: return ""
    if isinstance(x,(list,tuple,set)): return "、".join(map(_to_text,x))
    if isinstance(x,dict): return "、".join(f"{k}:{_to_text(v)}" for k,v in x.items())
    return str(x)

def _search(cur,q,limit=40):
    if q:
        cur.execute("""select url,title,summary,rate,cap,target,cost_items,deadline,fiscal_year,
                              call_no,scheme_type,period_from,period_to,last_fetched
                         from public.pages
                        where tokens @@ plainto_tsquery('simple', %s)
                     order by last_fetched desc limit %s""",(q,limit))
    else:
        cur.execute("""select url,title,summary,rate,cap,target,cost_items,deadline,fiscal_year,
                              call_no,scheme_type,period_from,period_to,last_fetched
                         from public.pages
                     order by last_fetched desc limit %s""",(limit,))
    cols=[d.name for d in cur.description]
    return [dict(zip(cols,row)) for row in cur]

def _llm_batch(items,profile):
    # 上位Nのみ採点（まとめて1回）
    t=[]
    for i,it in enumerate(items):
        txt=" ".join(_to_text(it.get(k)) for k in ("title","summary","target","cost_items","rate","cap"))
        t.append({"idx":i,"text":txt[:1200]})
    prompt=("あなたは補助金マッチングの査定者。各候補 idx ごとに0〜100点と2〜4個の理由をJSON配列で返す。\n"
            "出力例: [{\"idx\":0,\"score\":80,\"reasons\":[\"...\"]}, ...]\n"
            f"[事業者]\n{json.dumps(profile,ensure_ascii=False)}\n[候補]\n{json.dumps(t,ensure_ascii=False)}")
    try:
        r=client.chat.completions.create(model=OPENAI_MODEL,
             messages=[{"role":"user","content":prompt}], temperature=0.2, max_tokens=800)
        txt=r.choices[0].message.content.strip()
        m=re.search(r"\[[\s\S]*\]", txt)
        if not m: return
        arr=json.loads(m.group(0))
        for obj in arr:
            i=obj.get("idx"); 
            if isinstance(i,int) and 0<=i<len(items):
                items[i]["score"]=float(obj.get("score",50))
                items[i]["why"]=[_norm(x) for x in obj.get("reasons",[]) if x]
    except Exception as e:
        for it in items: it.setdefault("why",[]).append(f"llm error: {e}")

@app.post("/")
def handler():
    d=request.get_json(force=True) or {}
    profile={"所在地_都道府県":d.get("prefecture",""),
             "目的":d.get("goal",""),
             "対象経費カテゴリ":d.get("cost_categories",[])}
    q=d.get("query") or None
    MAX_LLM=int(os.getenv("MAX_LLM_ITEMS","6"))
    rows=[]
    with psycopg.connect(DSN, autocommit=True) as c, c.cursor() as cur:
        rows=_search(cur,q,limit=int(os.getenv("LIST_LIMIT","30")))
    # まず軽量スコア（簡易）
    for it in rows:
        base=40.0
        qstr=" ".join([_to_text(profile.get("目的"))]+[_to_text(x) for x in profile.get("対象経費カテゴリ",[])])
        txt=_to_text(it.get("title"))+" "+_to_text(it.get("summary"))
        if qstr: base += 6.0*sum(1 for w in set(qstr.split()) if w and w in txt)
        base += 2.0 if it.get("rate") else 0.0; base += 2.0 if it.get("cap") else 0.0
        it["score"]=min(base,80.0); it.setdefault("why",[])
    # 上位Nだけ LLM バッチ採点
    top=rows[:MAX_LLM]; _llm_batch(top,profile)
    # 仕上げ
    for it in rows:
        it.setdefault("why_table",[
            {"項目":"所在地","入力":_to_text(profile.get("所在地_都道府県")),"制度側":_to_text(it.get("target")),"評価":"-"},
            {"項目":"目的","入力":_to_text(profile.get("目的")),"制度側":_to_text(it.get("summary")),"評価":"-"},
            {"項目":"対象経費","入力":_to_text(profile.get("対象経費カテゴリ")),"制度側":_to_text(it.get("cost_items")),"評価":"-"},
        ])
        lf=it.pop("last_fetched",None)
        if lf: it["last_checked_at"]=str(lf)
    rows.sort(key=lambda x:(-(x.get("score") or 0), _norm(x.get("title"))))
    return jsonify({"items":rows,"kpi":{"elapsed_ms":0}})
# Cloud Functions (gen2) entry: app
