begin;

insert into core.sources (id, category, aliases)
values ('google_news', 'vendor', array['news.google.com'])
on conflict (id) do update
set category = excluded.category,
    aliases = excluded.aliases,
    updated_at = now();

commit;
