-- ログは14日で削除
delete from fetch_log where fetched_at < now() - interval '14 days';

-- ページは90日超で削除
delete from pages where last_fetched < now() - interval '90 days';

-- ドメインごとの上位Nだけ残す（新しい順）。N=120の例
with ranked as (
  select url,
         row_number() over (
           partition by split_part(split_part(url,'//',2), '/', 1)
           order by last_fetched desc
         ) as rn
  from pages
)
delete from pages p using ranked r
where p.url=r.url and r.rn > 120;

-- http_cache は最近2年で残す（必要ならコメントアウト）
delete from http_cache where last_checked_at < now() - interval '2 years';