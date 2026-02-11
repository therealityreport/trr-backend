begin;

insert into core.sources (id, category, aliases)
values ('bravo', 'vendor', array['bravotv'])
on conflict (id) do update
set category = excluded.category,
    aliases = excluded.aliases,
    updated_at = now();

commit;
