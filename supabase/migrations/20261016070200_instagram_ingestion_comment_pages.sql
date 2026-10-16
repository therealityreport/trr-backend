begin;

-- Page-level progress is distinct from a target settlement receipt. Evidence is
-- retained before parsing; only a valid canonical page receives this receipt.
create table social.instagram_ingestion_comment_pages (
  page_id uuid primary key,
  run_id uuid not null,
  stage text not null default 'comments' check (stage='comments'),
  target_key text not null,
  owner_generation bigint not null,
  evidence_id uuid not null,
  observed_at timestamptz not null,
  content_hash text not null,
  parent_comment_id text,
  next_cursor text,
  exhausted boolean not null,
  canonical_identity jsonb not null,
  checkpoint jsonb not null,
  committed_at timestamptz not null default clock_timestamp(),
  foreign key (run_id,stage,target_key)
    references social.instagram_ingestion_targets(run_id,stage,target_key),
  foreign key (evidence_id,run_id,stage,target_key)
    references social.instagram_ingestion_evidence(evidence_id,run_id,stage,target_key),
  unique (evidence_id)
);
create index instagram_ingestion_comment_pages_target_idx
  on social.instagram_ingestion_comment_pages(run_id,target_key,committed_at);
alter table social.instagram_ingestion_comment_pages enable row level security;
revoke all on social.instagram_ingestion_comment_pages from public,anon,authenticated;
grant all on social.instagram_ingestion_comment_pages to service_role;

commit;
