begin;

alter table core.shows
  add column if not exists imdb_meta jsonb;

commit;
