-- pages / fetch_log / http_cache は既にある想定（省略可）

create table if not exists public.pages(
  url           text primary key,
  title         text not null,
  summary       text,
  rate          text,
  cap           text,
  target        text,
  cost_items    text,
  deadline      text,
  fiscal_year   text,
  call_no       text,
  scheme_type   text,
  period_from   text,
  period_to     text,
  content_hash  text,
  last_fetched  timestamptz default now()
);

do $$ begin
  if not exists (
    select 1 from information_schema.columns
     where table_schema='public' and table_name='pages' and column_name='tokens'
  ) then
    alter table public.pages add column tokens tsvector generated always as
      (to_tsvector('simple',
        coalesce(title,'')||' '||coalesce(summary,'')||' '||
        coalesce(target,'')||' '||coalesce(cost_items,''))) stored;
    create index if not exists idx_pages_tokens on public.pages using gin(tokens);
    create index if not exists idx_pages_last   on public.pages(last_fetched desc);
  end if;
end $$;

create table if not exists public.fetch_log(
  id         bigserial primary key,
  url        text,
  status     text,        -- ok / 304 / skip / ng / list
  took_ms    integer,
  error      text,
  fetched_at timestamptz default now()
);

create table if not exists public.http_cache(
  url             text primary key,
  etag            text,
  last_modified   text,
  last_status     integer,
  last_checked_at timestamptz,
  last_changed_at timestamptz
);

-- 月次クォータ（予約語回避のため quota_limit 列名を採用）
create table if not exists public.api_quota(
  month       text not null,   -- '2025-09'
  api         text not null,   -- 'vertex' / 'bing' など
  used        integer not null default 0,
  quota_limit integer not null,
  primary key (month, api)
);

-- すでに "limit" 列で作成済みだった場合は安全にリネーム
do $$
begin
  if exists (
    select 1 from information_schema.columns
     where table_schema='public' and table_name='api_quota' and column_name='limit'
  ) then
    execute 'alter table public.api_quota rename column "limit" to quota_limit';
  end if;
end $$;
