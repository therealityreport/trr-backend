begin;

create schema if not exists extensions;

create extension if not exists index_advisor
with schema extensions;

comment on extension index_advisor is
  'Index recommendation helper used by TRR operator tooling; recommendations require separate review before DDL.';

commit;
