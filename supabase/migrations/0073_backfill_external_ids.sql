begin;

-- Shows
insert into core.show_external_ids (show_id, source_id, external_id, is_primary)
select id, 'imdb', imdb_id, true
from core.shows
where imdb_id is not null and btrim(imdb_id) <> ''
on conflict do nothing;

insert into core.show_external_ids (show_id, source_id, external_id, is_primary)
select id, 'tmdb', tmdb_id::text, true
from core.shows
where tmdb_id is not null
on conflict do nothing;

insert into core.show_external_ids (show_id, source_id, external_id, is_primary)
select id, 'tvdb', tvdb_id::text, true
from core.shows
where tvdb_id is not null
on conflict do nothing;

insert into core.show_external_ids (show_id, source_id, external_id, is_primary)
select id, 'tvrage', tvrage_id::text, true
from core.shows
where tvrage_id is not null
on conflict do nothing;

insert into core.show_external_ids (show_id, source_id, external_id, is_primary)
select id, 'wikidata', wikidata_id, true
from core.shows
where wikidata_id is not null and btrim(wikidata_id) <> ''
on conflict do nothing;

insert into core.show_external_ids (show_id, source_id, external_id, is_primary)
select id, 'facebook', facebook_id, true
from core.shows
where facebook_id is not null and btrim(facebook_id) <> ''
on conflict do nothing;

insert into core.show_external_ids (show_id, source_id, external_id, is_primary)
select id, 'instagram', instagram_id, true
from core.shows
where instagram_id is not null and btrim(instagram_id) <> ''
on conflict do nothing;

insert into core.show_external_ids (show_id, source_id, external_id, is_primary)
select id, 'twitter', twitter_id, true
from core.shows
where twitter_id is not null and btrim(twitter_id) <> ''
on conflict do nothing;

-- Seasons (typed + JSONB)
insert into core.season_external_ids (season_id, source_id, external_id, is_primary)
select id, 'tmdb', tmdb_season_id::text, true
from core.seasons
where tmdb_season_id is not null
on conflict do nothing;

insert into core.season_external_ids (season_id, source_id, external_id, is_primary)
select id, 'tvdb', external_tvdb_id::text, true
from core.seasons
where external_tvdb_id is not null
on conflict do nothing;

insert into core.season_external_ids (season_id, source_id, external_id, is_primary)
select id, 'wikidata', external_wikidata_id, true
from core.seasons
where external_wikidata_id is not null and btrim(external_wikidata_id) <> ''
on conflict do nothing;

insert into core.season_external_ids (season_id, source_id, external_id, is_primary)
select s.id, e.key, e.value, true
from core.seasons s
cross join lateral jsonb_each_text(s.external_ids) e(key, value)
where btrim(e.value) <> ''
on conflict do nothing;

-- Episodes (typed + JSONB)
insert into core.episode_external_ids (episode_id, source_id, external_id, is_primary)
select id, 'imdb', imdb_episode_id, true
from core.episodes
where imdb_episode_id is not null and btrim(imdb_episode_id) <> ''
on conflict do nothing;

insert into core.episode_external_ids (episode_id, source_id, external_id, is_primary)
select id, 'tmdb', tmdb_episode_id::text, true
from core.episodes
where tmdb_episode_id is not null
on conflict do nothing;

insert into core.episode_external_ids (episode_id, source_id, external_id, is_primary)
select e.id, j.key, j.value, true
from core.episodes e
cross join lateral jsonb_each_text(e.external_ids) j(key, value)
where btrim(j.value) <> ''
on conflict do nothing;

-- People (from core.people.external_ids JSONB)
insert into core.person_external_ids (person_id, source_id, external_id, is_primary)
select p.id, e.key, e.value, true
from core.people p
cross join lateral jsonb_each_text(p.external_ids) e(key, value)
where btrim(e.value) <> ''
on conflict do nothing;

-- Conflict capture example
insert into core.external_id_conflicts(entity_type, entity_id, source_id, external_id, conflict_reason)
select 'show', s.id, 'imdb', s.imdb_id, 'existing primary differs'
from core.shows s
join core.show_external_ids x
  on x.show_id = s.id and x.source_id = 'imdb' and x.external_id <> s.imdb_id
where s.imdb_id is not null;

commit;
