from __future__ import annotations
import os, re, time, threading, yaml
from typing import List, Set
from urllib.parse import urlsplit, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

from lib.http_client import conditional_fetch
from lib.extractors import extract_from_html, extract_from_text
from lib.db import conn, upsert_http_meta, upsert_page, log_fetch, ensure_schema

try:
    from tavily import TavilyClient
except Exception:
    TavilyClient = None
TAVILY_KEY = os.getenv("TAVILY_API_KEY")
tv = TavilyClient(api_key=TAVILY_KEY) if (TAVILY_KEY and TavilyClient) else None

TIME_BUDGET_SEC   = int(os.getenv("TIME_BUDGET_SEC", "480"))
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN", "120"))
MAX_PER_DOMAIN    = int(os.getenv("MAX_PER_DOMAIN", "50"))
PARALLEL_WORKERS  = int(os.getenv("PARALLEL_WORKERS", "6"))
PER_HOST_LIMIT    = int(os.getenv("PER_HOST_LIMIT", "2"))
DOC_TYPES: Set[str] = {"text/html", "application/xhtml+xml", "application/pdf"}
ASSET_RE  = re.compile(r'\.(js|mjs|css|png|jpe?g|gif|svg|ico|json|map|woff2?|ttf|eot|mp4|webm)($|\?)', re.I)

ALLOWED_HOSTS: Set[str] = set()
RUN_ID = os.getenv("RUN_ID","")

def allowed(u: str) -> bool:
    try: host=urlsplit(u).netloc
    except Exception: return False
    return any(host==d or host.endswith(d) for d in ALLOWED_HOSTS)

def is_document_url(u: str) -> bool:
    if not u.startswith(("http://","https://")): return False
    if ASSET_RE.search(u): return False
    return True

def load_seeds(path="seeds.yaml")->List[dict]:
    with open(path,"r",encoding="utf-8") as f:
        cfg=yaml.safe_load(f)
    global ALLOWED_HOSTS
    ALLOWED_HOSTS=set(cfg.get("allowed_hosts",[]))
    return cfg.get("sources",[])

def L(c, url, status, took, msg=None):
    if RUN_ID: msg = f"run={RUN_ID}; " + (msg or "")
    log_fetch(c, url, status, took, msg)

def extract_links(base_url: str, html: str) -> List[str]:
    soup=BeautifulSoup(html, "html.parser")
    out=[]
    for a in soup.find_all("a", href=True):
        href=a.get("href")
        if not href or href.startswith("#") or href.startswith("javascript:"): continue
        out.append(urljoin(base_url, href))
    seen=set(); uniq=[]
    for u in out:
        if u not in seen: seen.add(u); uniq.append(u)
    return uniq

URL_RE=re.compile(r'https?://(?:www\.)?(?:chusho\.meti\.go\.jp|meti\.go\.jp|jgrants-portal\.go\.jp)[^\s"\'>)]+', re.I)
def extract_links_by_regex(html: str)->List[str]:
    out=URL_RE.findall(html or "")
    seen=set(); uniq=[]
    for u in out:
        if u not in seen: seen.add(u); uniq.append(u)
    return uniq

_saved=0; _lock=threading.Lock()
host_sem:dict[str,threading.Semaphore]={}; host_lock=threading.Lock()
def _host_sem(host:str):
    with host_lock:
        if host not in host_sem: host_sem[host]=threading.Semaphore(PER_HOST_LIMIT)
        return host_sem[host]
def _inc(n=1):
    global _saved
    with _lock: _saved += n

def process_detail(u:str, deadline:float)->None:
    if time.time()>deadline:
        with conn() as c: L(c,u,"skip",0,"deadline"); return
    host=urlsplit(u).netloc
    with _host_sem(host):
        try:
            with conn() as c:
                cur=c.cursor()
                cur.execute("select etag, last_modified from public.http_cache where url=%s",(u,), prepare=False)
                petag,plm=cur.fetchone() or (None,None)
                html,new_etag,new_lm,ctype,status,took=conditional_fetch(u,petag,plm)
                upsert_http_meta(c,u,new_etag,new_lm,status)
                if html is None: L(c,u,"304",took,None); return
                if ctype and ctype.lower() not in DOC_TYPES:
                    L(c,u,"skip",took,f"ctype={ctype}"); return
                row=extract_from_html(u,html)
                changed=upsert_page(c,row)
                L(c,u,"ok" if changed else "skip",took,None)
                if changed: _inc(1); return
        except Exception as e:
            if tv:
                try:
                    raw=None
                    if hasattr(tv,"extract"): raw=tv.extract(u).get("content")  # type: ignore[attr-defined]
                    if not raw:
                        r=tv.search(u, search_depth="basic", max_results=1,
                                    include_answer=False, include_raw_content=True)
                        raw=(r.get("results") or [{}])[0].get("raw_content")
                    if raw:
                        with conn() as c:
                            row=extract_from_text(u,raw)
                            changed=upsert_page(c,row)
                            L(c,u,"ok" if changed else "skip",0,"fallback: raw")
                            if changed: _inc(1); return
                except Exception as e2:
                    with conn() as c: L(c,u,"ng",0,f"fallback error: {e2}"); return
            with conn() as c: L(c,u,"ng",0,str(e)); return

def crawl()->None:
    ensure_schema()
    sources=load_seeds()
    deadline=time.time()+TIME_BUDGET_SEC
    global _saved; _saved=0
    per_domain:dict[str,int]={}

    with conn() as c_main:
        for src in sources:
            if time.time()>deadline: break
            list_url=src["url"]; include=[re.compile(p) for p in src.get("include",[])]; exclude=[re.compile(p) for p in src.get("exclude",[])]
            max_new=int(src.get("max_new",20))

            cur=c_main.cursor()
            cur.execute("select etag, last_modified from public.http_cache where url=%s",(list_url,), prepare=False)
            etag,lm=cur.fetchone() or (None,None)
            html=None; ctype=None
            try:
                html,new_etag,new_lm,ctype,status,took=conditional_fetch(list_url,etag,lm)
                upsert_http_meta(c_main,list_url,new_etag,new_lm,status)
            except Exception as e:
                L(c_main,list_url,"ng",0,f"list error: {e}")

            anchors=extract_links(list_url,html) if html and (not ctype or ctype.lower() in DOC_TYPES) else []
            regex_found=extract_links_by_regex(html or "")

            def ok(u:str)->bool:
                if include and not any(p.search(u) for p in include): return False
                if exclude and any(p.search(u) for p in exclude): return False
                return allowed(u) and is_document_url(u)

            cand=[u for u in (anchors+regex_found) if ok(u)]
            seen=set(); uniq=[]
            for u in cand:
                if u not in seen: seen.add(u); uniq.append(u)
            cand=uniq

            filtered=[]
            for u in cand:
                host=urlsplit(u).netloc; cnt=per_domain.get(host,0)
                if cnt<MAX_PER_DOMAIN: filtered.append(u); per_domain[host]=cnt+1
                if len(filtered)>=max_new: break

            L(c_main,list_url,"list",0,f"anchors={len(anchors)}, regex={len(regex_found)}, candidates={len(filtered)}")
            if not filtered: continue

            with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
                futures=[ex.submit(process_detail,u,deadline) for u in filtered]
                for _ in as_completed(futures):
                    if time.time()>deadline or _saved>=MAX_PAGES_PER_RUN:
                        for f in futures: f.cancel()
                        break

            if time.time()>deadline or _saved>=MAX_PAGES_PER_RUN: break
