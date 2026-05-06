begin;

-- Additive Instagram comment API phase metadata. These columns preserve
-- upstream ordering, cursor provenance, status, visibility, and reply-count
-- evidence without changing the existing (post_id, comment_id) upsert contract.

alter table social.instagram_comments
  add column if not exists is_covered boolean not null default false,
  add column if not exists is_ranked boolean not null default false,
  add column if not exists comment_index integer,
  add column if not exists phase text,
  add column if not exists did_report_as_spam boolean not null default false,
  add column if not exists status text not null default 'Active',
  add column if not exists is_edited boolean not null default false,
  add column if not exists is_pinned boolean not null default false,
  add column if not exists meta_ai_comment_type text not null default 'NONE',
  add column if not exists child_comment_count integer not null default 0,
  add column if not exists liked_by_media_coauthors boolean not null default false,
  add column if not exists cursor_min_id text,
  add column if not exists cursor_param text,
  add column if not exists cursor_payload jsonb not null default '{}'::jsonb,
  add column if not exists comment_filter_param text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'social.instagram_comments'::regclass
      and conname = 'instagram_comments_phase_check'
  ) then
    alter table social.instagram_comments
      add constraint instagram_comments_phase_check
      check (phase is null or phase in ('ranked', 'headload', 'fb_crosspost', 'child'))
      not valid;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'social.instagram_comments'::regclass
      and conname = 'instagram_comments_cursor_param_check'
  ) then
    alter table social.instagram_comments
      add constraint instagram_comments_cursor_param_check
      check (
        cursor_param is null
        or cursor_param in (
          'min_id',
          'max_id',
          'cached_comments_cursor',
          'bifilter_token',
          'tao_cursor'
        )
      )
      not valid;
  end if;
end $$;

create index if not exists instagram_comments_post_phase_comment_index_idx
  on social.instagram_comments (post_id, phase, comment_index);

create index if not exists instagram_comments_post_covered_status_idx
  on social.instagram_comments (post_id)
  where is_covered or status <> 'Active';

create index if not exists instagram_comments_post_child_count_idx
  on social.instagram_comments (post_id, child_comment_count)
  where child_comment_count > 0;

comment on column social.instagram_comments.is_covered is
  'True when Instagram reports the comment as covered or masked by an upstream filter.';

comment on column social.instagram_comments.is_ranked is
  'True when the row was observed in Instagram ranked/top-load ordering; preserve ranked ordinals when later pages repeat the comment.';

comment on column social.instagram_comments.comment_index is
  'Upstream comment ordinal within the producing phase, especially ranked ordering; null when unavailable.';

comment on column social.instagram_comments.phase is
  'Instagram comment capture phase that produced this row: ranked, headload, fb_crosspost, or child.';

comment on column social.instagram_comments.reply_count is
  'Legacy scraper-observed reply count from comment payloads; use child_comment_count for Instagram authoritative child totals when present.';

comment on column social.instagram_comments.child_comment_count is
  'Authoritative child-comment total reported by Instagram for the comment, not the number of replies already persisted locally.';

comment on column social.instagram_comments.cursor_min_id is
  'Cursor value used for min_id-compatible pagination when the comment page was captured.';

comment on column social.instagram_comments.cursor_param is
  'Name of the Instagram cursor parameter that produced the row: min_id, max_id, cached_comments_cursor, bifilter_token, or tao_cursor.';

comment on column social.instagram_comments.cursor_payload is
  'Sanitized cursor envelope returned by Instagram for the page that yielded this comment; excludes cookies, headers, and secrets.';

comment on column social.instagram_comments.comment_filter_param is
  'Instagram comment filter mode requested for the page, such as default, hidden, or covered-comment retrieval.';

comment on column social.instagram_comments.status is
  'Instagram-reported comment status, defaulting to Active when the API omits status details.';

comment on column social.instagram_comments.did_report_as_spam is
  'Whether Instagram reports that the viewer/account marked this comment as spam.';

comment on column social.instagram_comments.is_edited is
  'Whether Instagram reports the comment text was edited after initial creation.';

comment on column social.instagram_comments.is_pinned is
  'Whether Instagram reports the comment is pinned on the media.';

comment on column social.instagram_comments.meta_ai_comment_type is
  'Instagram Meta AI comment type marker, defaulting to NONE when absent.';

comment on column social.instagram_comments.liked_by_media_coauthors is
  'Whether Instagram reports the comment was liked by media coauthors.';

create or replace view social.comment_capture_health as
with comment_rollup as (
  select
    post_id,
    count(*) filter (where phase is distinct from 'fb_crosspost')::bigint as saved_comment_count,
    count(*) filter (
      where phase is distinct from 'fb_crosspost'
        and parent_comment_id is null
    )::bigint as saved_parent_comments,
    count(*) filter (
      where phase is distinct from 'fb_crosspost'
        and (parent_comment_id is not null or is_reply)
    )::bigint as saved_child_replies,
    count(*) filter (where phase = 'ranked')::bigint as phase_ranked_count,
    count(*) filter (where phase = 'headload')::bigint as phase_headload_count,
    count(*) filter (where phase = 'fb_crosspost')::bigint as phase_fb_crosspost_count,
    count(*) filter (where phase = 'child')::bigint as phase_child_count,
    count(*) filter (where phase is null)::bigint as phase_unknown_count,
    count(*) filter (where is_covered)::bigint as covered_comment_count,
    count(*) filter (where did_report_as_spam)::bigint as spam_report_count,
    count(*) filter (where status <> 'Active')::bigint as inactive_status_count,
    coalesce(sum(child_comment_count), 0)::bigint as reported_child_comment_total,
    max(scraped_at) as last_comment_scraped_at
  from social.instagram_comments
  group by post_id
),
status_rollup as (
  select
    post_id,
    jsonb_object_agg(status, status_count order by status) as status_counts
  from (
    select
      post_id,
      status,
      count(*)::integer as status_count
    from social.instagram_comments
    group by post_id, status
  ) counts
  group by post_id
)
select
  p.id as post_id,
  p.shortcode,
  p.media_id,
  p.username,
  p.source_account,
  p.season_id,
  p.job_id,
  p.comments_count as instagram_reported_comments,
  coalesce(p.fb_comment_count, 0) as facebook_reported_comments,
  coalesce(c.saved_comment_count, 0) as saved_comment_count,
  coalesce(c.saved_parent_comments, 0) as saved_parent_comments,
  coalesce(c.saved_child_replies, 0) as saved_child_replies,
  coalesce(c.phase_ranked_count, 0) as phase_ranked_count,
  coalesce(c.phase_headload_count, 0) as phase_headload_count,
  coalesce(c.phase_fb_crosspost_count, 0) as phase_fb_crosspost_count,
  coalesce(c.phase_child_count, 0) as phase_child_count,
  coalesce(c.phase_unknown_count, 0) as phase_unknown_count,
  coalesce(c.covered_comment_count, 0) as covered_comment_count,
  coalesce(c.spam_report_count, 0) as spam_report_count,
  coalesce(c.inactive_status_count, 0) as inactive_status_count,
  coalesce(s.status_counts, '{}'::jsonb) as status_counts,
  coalesce(c.reported_child_comment_total, 0) as reported_child_comment_total,
  greatest(
    coalesce(p.comments_count, 0)::bigint
      - coalesce(c.saved_parent_comments, 0),
    0::bigint
  ) as parent_capture_gap,
  case
    when coalesce(p.comments_count, 0) > 0 then
      round(
        (
          coalesce(c.saved_parent_comments, 0)::numeric
          / p.comments_count::numeric
        ) * 100,
        2
      )
    else null
  end as parent_capture_rate_pct,
  c.last_comment_scraped_at,
  p.scraped_at as post_scraped_at
from social.instagram_posts p
left join comment_rollup c on c.post_id = p.id
left join status_rollup s on s.post_id = p.id;

alter view social.comment_capture_health set (security_invoker = on);

comment on view social.comment_capture_health is
  'Per-Instagram-post comment capture health, including phase counts, covered/status counts, and parent capture gap evidence.';

create table if not exists social.instagram_post_comments_audit (
  id uuid primary key default gen_random_uuid(),
  scrape_run_id uuid references social.scrape_runs(id) on delete set null,
  scrape_job_id uuid references social.scrape_jobs(id) on delete set null,
  post_id uuid not null references social.instagram_posts(id) on delete cascade,
  shortcode text,
  source_account text,
  reported_comment_count integer not null default 0 check (reported_comment_count >= 0),
  reported_fb_comment_count integer not null default 0 check (reported_fb_comment_count >= 0),
  fetched_comment_count integer not null default 0 check (fetched_comment_count >= 0),
  fetched_parent_comment_count integer not null default 0 check (fetched_parent_comment_count >= 0),
  fetched_child_comment_count integer not null default 0 check (fetched_child_comment_count >= 0),
  phase_ranked_count integer not null default 0 check (phase_ranked_count >= 0),
  phase_headload_count integer not null default 0 check (phase_headload_count >= 0),
  phase_fb_crosspost_count integer not null default 0 check (phase_fb_crosspost_count >= 0),
  phase_child_count integer not null default 0 check (phase_child_count >= 0),
  phase_counts jsonb not null default '{}'::jsonb,
  covered_comment_count integer not null default 0 check (covered_comment_count >= 0),
  spam_report_count integer not null default 0 check (spam_report_count >= 0),
  inactive_status_count integer not null default 0 check (inactive_status_count >= 0),
  status_counts jsonb not null default '{}'::jsonb,
  cursor_stop_reason text,
  cursor_min_id text,
  cursor_param text check (
    cursor_param is null
    or cursor_param in (
      'min_id',
      'max_id',
      'cached_comments_cursor',
      'bifilter_token',
      'tao_cursor'
    )
  ),
  cursor_payload jsonb not null default '{}'::jsonb,
  comment_filter_param text,
  reported_parent_gap_count integer not null default 0 check (reported_parent_gap_count >= 0),
  reported_child_gap_count integer not null default 0 check (reported_child_gap_count >= 0),
  reported_total_gap_count integer not null default 0 check (reported_total_gap_count >= 0),
  created_at timestamptz not null default now()
);

create index if not exists instagram_post_comments_audit_post_created_idx
  on social.instagram_post_comments_audit (post_id, created_at desc);

create index if not exists instagram_post_comments_audit_run_job_created_idx
  on social.instagram_post_comments_audit (scrape_run_id, scrape_job_id, created_at desc)
  where scrape_run_id is not null or scrape_job_id is not null;

grant select on table social.comment_capture_health to anon, authenticated, service_role;
grant all privileges on table social.instagram_post_comments_audit to service_role;

alter table social.instagram_post_comments_audit enable row level security;

comment on table social.instagram_post_comments_audit is
  'Per-run Instagram comment capture audit snapshots for post totals, phase totals, cursor stop reasons, status counts, and gap evidence.';

comment on column social.instagram_post_comments_audit.scrape_run_id is
  'Optional scrape run that produced this comment audit snapshot.';

comment on column social.instagram_post_comments_audit.scrape_job_id is
  'Optional comments scrape job that produced this audit snapshot.';

comment on column social.instagram_post_comments_audit.phase_counts is
  'Phase totals serialized from the worker when scalar phase columns are not enough for diagnostics.';

comment on column social.instagram_post_comments_audit.status_counts is
  'Comment status totals observed during the audit snapshot.';

comment on column social.instagram_post_comments_audit.cursor_stop_reason is
  'Worker stop reason for the cursor loop, such as exhausted, budget_exhausted, checkpoint_required, or error.';

comment on column social.instagram_post_comments_audit.cursor_payload is
  'Sanitized final cursor envelope for the audited post; excludes cookies, headers, and secrets.';

comment on column social.instagram_post_comments_audit.comment_filter_param is
  'Instagram comment filter mode used for the audited fetch.';

commit;
