begin;

-- Supabase Performance Advisor flags exposed tables without a primary key.
-- This is conflict-staging data with no stable natural key, so use a
-- defaulted surrogate key and keep existing insert callers unchanged.
alter table core.external_id_conflicts
  add column if not exists id uuid;

alter table core.external_id_conflicts
  alter column id set default gen_random_uuid();

update core.external_id_conflicts
set id = gen_random_uuid()
where id is null;

alter table core.external_id_conflicts
  alter column id set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'core.external_id_conflicts'::regclass
      and contype = 'p'
  ) then
    alter table core.external_id_conflicts
      add constraint external_id_conflicts_pkey primary key (id);
  end if;
end $$;

commit;
