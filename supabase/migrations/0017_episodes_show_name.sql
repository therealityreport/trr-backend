begin;

-- Denormalized show name on core.episodes (requested).
-- Note: Postgres cannot reorder columns; this adds `show_name` as a column and keeps it in sync via triggers.

create schema if not exists core;

alter table core.episodes
  add column if not exists show_name text;

-- Backfill for existing rows.
update core.episodes ep
set show_name = sh.title
from core.shows sh
where ep.show_id = sh.id
  and (ep.show_name is null or btrim(ep.show_name) = '');

-- Keep show_name populated on insert/update.
create or replace function core.set_episode_show_name()
returns trigger
language plpgsql
as $$
begin
  select s.title into new.show_name
  from core.shows s
  where s.id = new.show_id;
  return new;
end;
$$;

drop trigger if exists core_episodes_set_show_name on core.episodes;
create trigger core_episodes_set_show_name
before insert or update on core.episodes
for each row
execute function core.set_episode_show_name();

-- Propagate show title changes to existing episodes (best-effort drift prevention).
create or replace function core.propagate_show_title_to_episodes()
returns trigger
language plpgsql
as $$
begin
  update core.episodes
  set show_name = new.title
  where show_id = new.id
    and show_name is distinct from new.title;
  return new;
end;
$$;

drop trigger if exists core_shows_propagate_title_to_episodes on core.shows;
create trigger core_shows_propagate_title_to_episodes
after update of title on core.shows
for each row
execute function core.propagate_show_title_to_episodes();

commit;

