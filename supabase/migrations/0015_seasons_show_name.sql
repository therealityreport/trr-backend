begin;

-- Denormalized show name on core.seasons (requested).
-- Note: Postgres cannot reorder columns; this adds `show_name` as a column and keeps it in sync via triggers.

create schema if not exists core;

alter table core.seasons
  add column if not exists show_name text;

-- Backfill for existing rows.
update core.seasons se
set show_name = sh.title
from core.shows sh
where se.show_id = sh.id
  and (se.show_name is null or btrim(se.show_name) = '');

-- Keep show_name populated on insert/update.
create or replace function core.set_season_show_name()
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

drop trigger if exists core_seasons_set_show_name on core.seasons;
create trigger core_seasons_set_show_name
before insert or update on core.seasons
for each row
execute function core.set_season_show_name();

-- Propagate show title changes to existing seasons (best-effort drift prevention).
create or replace function core.propagate_show_title_to_seasons()
returns trigger
language plpgsql
as $$
begin
  update core.seasons
  set show_name = new.title
  where show_id = new.id
    and show_name is distinct from new.title;
  return new;
end;
$$;

drop trigger if exists core_shows_propagate_title_to_seasons on core.shows;
create trigger core_shows_propagate_title_to_seasons
after update of title on core.shows
for each row
execute function core.propagate_show_title_to_seasons();

commit;

