from http.server import BaseHTTPRequestHandler
import os, json, time
USE_DB   = bool(os.getenv("DATABASE_URL"))
PREFER_DB= os.getenv("PREFER_DB","1") == "1"

from core_cached import recommend_from_db
from core import recommend as recommend_live

def _send(r, code, obj):
    b=json.dumps(obj,ensure_ascii=False).encode("utf-8")
    r.send_response(code); r.send_header("Content-Type","application/json; charset=utf-8")
    r.send_header("Content-Length", str(len(b))); r.end_headers(); r.wfile.write(b)

def _json(r):
    try:
        n=int(r.headers.get("content-length","0"))
        return json.loads(r.rfile.read(n) or b"{}")
    except: return {}

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        d=_json(self)
        profile = {
          "社名": d.get("company_name",""),
          "所在地_都道府県": d.get("prefecture",""),
          "所在地_市区町村": d.get("city",""),
          "業種": d.get("industry",""), "業種_分類": d.get("industry_class",""),
          "従業員規模": d.get("size",""), "法人形態": d.get("corp_form",""),
          "目的": d.get("goal",""), "課題": d.get("pain",""),
          "対象経費カテゴリ": d.get("cost_categories",[]),
          "CAPEX": d.get("capex",""), "OPEX": d.get("opex",""),
        }
        query  = d.get("query") or None
        scope  = d.get("scope") or "national"
        nocache= (str(d.get("nocache","0"))=="1")
        t0=time.time()
        try:
            if USE_DB and PREFER_DB:
                res = recommend_from_db(profile, query=query)
            else:
                res = recommend_live(profile, query=query, scope=scope, force_refresh=nocache)
        except Exception as e:
            res={"items":[], "excluded":[{"title":"","url":"","reason":str(e)}], "kpi":{"elapsed_ms":0}}
        res.setdefault("kpi",{})["elapsed_ms"]=int((time.time()-t0)*1000)
        _send(self,200,res)

    def do_GET(self):
        _send(self,200,{"ok":True})