-- Migration 0113: Extend social.scrape_jobs platform support to include reddit
--
-- social.scrape_jobs was created with an unnamed CHECK constraint on platform.
-- This migration drops the existing platform CHECK (whatever its generated name)
-- and replaces it with a named constraint that includes 'reddit'.

begin;

do $$
declare
  r record;
begin
  for r in
    select
      c.conname,
      pg_get_constraintdef(c.oid) as def
    from pg_constraint c
    where c.conrelid = 'social.scrape_jobs'::regclass
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%platform%'
      and pg_get_constraintdef(c.oid) ilike '%instagram%'
      and pg_get_constraintdef(c.oid) ilike '%twitter%'
  loop
    execute format('alter table social.scrape_jobs drop constraint %I', r.conname);
  end loop;
end $$;

alter table social.scrape_jobs
  add constraint scrape_jobs_platform_check
  check (platform in ('instagram', 'tiktok', 'youtube', 'twitter', 'reddit'));

commit;

