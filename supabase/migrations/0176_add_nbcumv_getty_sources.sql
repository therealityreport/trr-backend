begin;

insert into core.sources (id, category, aliases)
values
  ('nbcumv', 'vendor', array['nbcu', 'nbcumv', 'nbc media village']),
  ('getty', 'vendor', array['getty images', 'gettyimages'])
on conflict (id) do update
set category = excluded.category,
    aliases = excluded.aliases,
    updated_at = now();

commit;
