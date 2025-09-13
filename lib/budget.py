import os, datetime, psycopg
from contextlib import contextmanager
from lib.db import ensure_schema  # 追加：毎回先にスキーマ適用

DSN = os.getenv("DATABASE_URL")

@contextmanager
def _conn():
    with psycopg.connect(DSN, autocommit=True) as c:
        yield c

def _month_str(dt=None):
    dt = dt or datetime.datetime.utcnow()
    return dt.strftime("%Y-%m")

def set_monthly_limit(api: str, limit: int):
    ensure_schema()  # 先に適用（api_quota のリネームもここで入る）
    with _conn() as c:
        cur = c.cursor()
        cur.execute("""
          insert into public.api_quota(month, api, used, quota_limit)
          values (%s,%s,0,%s)
          on conflict (month, api) do update
            set quota_limit = excluded.quota_limit
        """, (_month_str(), api, limit))

def get_usage(api: str):
    with _conn() as c:
        cur = c.cursor()
        cur.execute("""
          select used, quota_limit
            from public.api_quota
           where month=%s and api=%s
        """, (_month_str(), api))
        row = cur.fetchone()
        return (row[0], row[1]) if row else (0, 0)

def can_spend(api: str, will_consume: int) -> bool:
    used, limit = get_usage(api)
    if limit == 0:   # 未初期化は保守的に拒否
        return False
    return used + will_consume <= limit

def add_usage(api: str, inc: int):
    with _conn() as c:
        cur = c.cursor()
        cur.execute("""
          insert into public.api_quota(month, api, used, quota_limit)
          values (%s,%s,%s,%s)
          on conflict (month, api) do update
            set used = public.api_quota.used + excluded.used
        """, (_month_str(), api, inc, 0))
