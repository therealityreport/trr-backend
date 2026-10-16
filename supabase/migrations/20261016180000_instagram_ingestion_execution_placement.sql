begin;

-- Execution location is mutable operator state, outside frozen source intent.
-- Old runs keep their exact intent/hash and retain the existing Modal path.
alter table social.instagram_ingestion_runs
  add column execution_placement text not null default 'modal'
    check (execution_placement in ('modal','local'));

comment on column social.instagram_ingestion_runs.execution_placement is
  'Durable fail-closed placement: local runs cannot be recovered or dispatched by Modal executors.';

commit;
