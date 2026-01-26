begin;

create extension if not exists pgcrypto;
create schema if not exists core;

create or replace function core.jsonb_sha256(p jsonb)
returns text
language sql
immutable
as $$
  select encode(digest(p::text, 'sha256'), 'hex');
$$;

commit;
