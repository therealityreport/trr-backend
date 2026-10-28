begin;

-- A held cancellation retains all business history and cannot become executable,
-- including when an older API does not know about the additive hold field.
alter table social.instagram_ingestion_runs
  add column execution_hold_reason text,
  add constraint instagram_ingestion_run_hold_cancelled_check check (
    execution_hold_reason is null or (
      state = 'cancelled' and execution_hold_reason ~ '^[a-z][a-z0-9_]{0,79}$'
    )
  );

commit;
