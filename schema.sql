-- pages: 抽出済みの要点のみ（生HTMLは保存しない）
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

-- 検索用トークン（生成列）
do $$ begin
  if not exists (
    select 1 from information_schema.columns
    where table_schema='public' and table_name='pages' and column_name='tokens'
  ) then
    alter table public.pages
      add column tokens tsvector generated always as
        (to_tsvector('simple',
          coalesce(title,'') || ' ' ||
          coalesce(summary,'') || ' ' ||
          coalesce(target,'') || ' ' ||
          coalesce(cost_items,''))) stored;
    create index if not exists idx_pages_tokens on public.pages using gin(tokens);
    create index if not exists idx_pages_last   on public.pages(last_fetched desc);
  end if;
end $$;

-- fetch_log: 取得履歴（PII なし）
create table if not exists public.fetch_log(
  id         bigserial primary key,
  url        text,
  status     text,        -- ok / 304 / skip / ng
  took_ms    integer,
  error      text,
  fetched_at timestamptz default now()
);

-- http_cache: 条件付きGETに使う ETag / Last-Modified
create table if not exists public.http_cache(
  url             text primary key,
  etag            text,
  last_modified   text,
  last_status     integer,
  last_checked_at timestamptz,
  last_changed_at timestamptz
);