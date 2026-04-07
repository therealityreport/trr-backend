begin;

alter table if exists ml.screentime_runs
  add column if not exists manifest_key text null;

commit;
