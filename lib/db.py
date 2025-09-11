import os, psycopg
from contextlib import contextmanager
from pathlib import Path

DSN = os.getenv("DATABASE_URL")

@contextmanager
def conn():
    with psycopg.connect(DSN, autocommit=True) as c:
        yield c

def ensure_schema():
    """
    schema.sql を適用（CREATE TABLE IF NOT EXISTS を想定）。
    何度呼んでも安全。
    """
    sql = Path("schema.sql").read_text(encoding="utf-8")
    with conn() as c:
        c.execute(sql)

def upsert_http_meta(c, url: str, etag: str | None, last_mod: str | None, status: int):
    cur = c.cursor()
    cur.execute("""
      insert into http_cache(url, etag, last_modified, last_status, last_checked_at, last_changed_at)
      values(%s,%s,%s,%s, now(), case when %s<>coalesce((select etag from http_cache where url=%s),'')
                                      or %s<>coalesce((select last_modified from http_cache where url=%s),'')
                                   then now() else coalesce((select last_changed_at from http_cache where url=%s), now()) end)
      on conflict(url) do update set
        etag=excluded.etag, last_modified=excluded.last_modified,
        last_status=excluded.last_status, last_checked_at=now();
    """, (url, etag, last_mod, status, etag or "", url, last_mod or "", url, url))

def log_fetch(c, url: str, status: str, took_ms: int, err: str | None=None):
    c.execute("insert into fetch_log(url,status,took_ms,error) values(%s,%s,%s,%s)",
              (url, status, took_ms, err))

# ---- ここからページ保存系 ----
from .util import content_hash

def upsert_page(c, row: dict):
    row = dict(row)
    row["content_hash"] = content_hash(row)
    cur = c.cursor()
    cur.execute("select content_hash from pages where url=%s", (row["url"],))
    prev = cur.fetchone()
    if prev and prev[0] == row["content_hash"]:
        return False  # 変化なし
    cols = ["url","title","summary","rate","cap","target","cost_items","deadline",
            "fiscal_year","call_no","scheme_type","period_from","period_to","content_hash"]
    vals = [row.get(k) for k in cols]
    cur.execute(
      f"""insert into pages({",".join(cols)})
          values({",".join(["%s"]*len(cols))})
          on conflict(url) do update set
            title=excluded.title, summary=excluded.summary, rate=excluded.rate,
            cap=excluded.cap, target=excluded.target, cost_items=excluded.cost_items,
            deadline=excluded.deadline, fiscal_year=excluded.fiscal_year,
            call_no=excluded.call_no, scheme_type=excluded.scheme_type,
            period_from=excluded.period_from, period_to=excluded.period_to,
            content_hash=excluded.content_hash, last_fetched=now()""",
      vals)
    return True