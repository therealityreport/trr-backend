begin;

-- Expand operator-owned local limits without changing immutable run intent or
-- existing policy rows. Admission budgets remain independent hard limits.
alter table social.instagram_ingestion_execution_policies
  drop constraint instagram_ingestion_execution_polici_comments_parallelism_check,
  add constraint instagram_ingestion_execution_policy_parallelism_check
    check (comments_parallelism in (1,2,4,6,8));

commit;
