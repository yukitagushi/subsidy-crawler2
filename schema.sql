-- 最小の抽出済テーブル（生HTMLは保存しない）
create table if not exists pages(
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
alter table pages
  add column if not exists tokens tsvector
  generated always as (
    to_tsvector('simple',
      coalesce(title,'') || ' ' ||
      coalesce(summary,'') || ' ' ||
      coalesce(target,'') || ' ' ||
      coalesce(cost_items,'')
    )
  ) stored;

create index if not exists idx_pages_tokens on pages using gin(tokens);
create index if not exists idx_pages_last   on pages(last_fetched desc);

-- URLごとのHTTPメタ（ETag/Last-Modified で条件付きGET）
create table if not exists http_cache(
  url             text primary key,
  etag            text,
  last_modified   text,
  last_status     integer,
  last_checked_at timestamptz,
  last_changed_at timestamptz
);

-- フェッチログ（軽量）
create table if not exists fetch_log(
  id         bigserial primary key,
  url        text,
  status     text,         -- "ok" / "ng" / "304" / "skip"
  took_ms    integer,
  error      text,
  fetched_at timestamptz default now()
);
create index if not exists idx_fetch_log_time on fetch_log(fetched_at desc);