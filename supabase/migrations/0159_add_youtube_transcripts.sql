alter table if exists social.youtube_videos
  add column if not exists transcript_text text,
  add column if not exists transcript_segments jsonb not null default '[]'::jsonb,
  add column if not exists transcript_language text,
  add column if not exists transcript_source text,
  add column if not exists transcript_synced_at timestamptz,
  add column if not exists transcript_error text;

