begin;

-- Coverage: show_images

do $$
declare missing int;
begin
  select count(*) into missing
  from core.show_images s
  where not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'show'
      and ml.entity_id = s.show_id
      and (ml.context->>'legacy_id')::uuid = s.id
  );

  if missing > 0 then
    raise exception 'Media backfill gate failed: % show_images rows missing media_links', missing;
  end if;
end $$;

-- Coverage: season_images

do $$
declare missing int;
begin
  select count(*) into missing
  from core.season_images s
  where not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'season'
      and ml.entity_id = s.season_id
      and (ml.context->>'legacy_id')::uuid = s.id
  );

  if missing > 0 then
    raise exception 'Media backfill gate failed: % season_images rows missing media_links', missing;
  end if;
end $$;

-- Coverage: episode_images

do $$
declare missing int;
begin
  select count(*) into missing
  from core.episode_images e
  where not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'episode'
      and ml.entity_id = e.episode_id
      and (ml.context->>'legacy_id')::uuid = e.id
  );

  if missing > 0 then
    raise exception 'Media backfill gate failed: % episode_images rows missing media_links', missing;
  end if;
end $$;

-- Coverage: person_images

do $$
declare missing int;
begin
  select count(*) into missing
  from core.person_images p
  where not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'person'
      and (ml.context->>'legacy_table') = 'person_images'
      and (ml.context->>'legacy_id')::uuid = p.id
  );

  if missing > 0 then
    raise exception 'Media backfill gate failed: % person_images rows missing media_links', missing;
  end if;
end $$;

-- Coverage: cast_photos

do $$
declare missing int;
begin
  select count(*) into missing
  from core.cast_photos c
  where not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'person'
      and (ml.context->>'legacy_table') = 'cast_photos'
      and (ml.context->>'legacy_id')::uuid = c.id
  );

  if missing > 0 then
    raise exception 'Media backfill gate failed: % cast_photos rows missing media_links', missing;
  end if;
end $$;

-- Dedupe: no duplicate hosted content

do $$
begin
  if exists (
    select 1
    from core.media_assets
    where hosted_sha256 is not null
    group by source, hosted_sha256
    having count(*) > 1
  ) then
    raise exception 'Media assets dedupe gate failed: duplicate hosted_sha256 found';
  end if;
end $$;

-- External ID uniqueness (strict sources only)

do $$
begin
  if exists (
    select 1
    from core.show_external_ids
    where source_id in ('imdb','tmdb','wikidata','tvdb','tvrage')
    group by source_id, external_id
    having count(*) > 1
  ) then
    raise exception 'External ID uniqueness gate failed for strict sources';
  end if;
end $$;

do $$
begin
  if exists (
    select 1
    from core.season_external_ids
    where source_id in ('imdb','tmdb','wikidata','tvdb','tvrage')
    group by source_id, external_id
    having count(*) > 1
  ) then
    raise exception 'Season external ID uniqueness gate failed for strict sources';
  end if;
end $$;

do $$
begin
  if exists (
    select 1
    from core.episode_external_ids
    where source_id in ('imdb','tmdb','wikidata','tvdb','tvrage')
    group by source_id, external_id
    having count(*) > 1
  ) then
    raise exception 'Episode external ID uniqueness gate failed for strict sources';
  end if;
end $$;

do $$
begin
  if exists (
    select 1
    from core.person_external_ids
    where source_id in ('imdb','tmdb','wikidata','tvdb','tvrage')
    group by source_id, external_id
    having count(*) > 1
  ) then
    raise exception 'Person external ID uniqueness gate failed for strict sources';
  end if;
end $$;

commit;
