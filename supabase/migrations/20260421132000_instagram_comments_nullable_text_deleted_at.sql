begin;

alter table social.instagram_comments
  alter column text drop not null;

alter table social.instagram_comments
  add column if not exists deleted_at timestamptz null;

create index if not exists instagram_comments_deleted_at_idx
  on social.instagram_comments (deleted_at)
  where deleted_at is not null;

commit;
