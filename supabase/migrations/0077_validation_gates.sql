begin;

-- Coverage: show_images

do $$
declare missing int;
begin
  select count(*) into missing
  from core.show_images s
  left join lateral (
    select a.id
    from core.media_assets a
    where a.source = s.source
      and (
        (a.hosted_sha256 is not null and a.hosted_sha256 = s.hosted_sha256)
        or (a.source_asset_id is not null and a.source_asset_id = s.source_image_id)
        or (a.source_url is not null and a.source_url = coalesce(s.url_original, s.url))
      )
    order by
      (a.hosted_sha256 is not null and a.hosted_sha256 = s.hosted_sha256) desc,
      (a.source_asset_id is not null and a.source_asset_id = s.source_image_id) desc,
      (a.source_url is not null and a.source_url = coalesce(s.url_original, s.url)) desc,
      a.id
    limit 1
  ) a on true
  where a.id is null
     or not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'show'
      and ml.entity_id = s.show_id
      and ml.kind = s.kind
      and ml.media_asset_id = a.id
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
  left join lateral (
    select a.id
    from core.media_assets a
    where a.source = s.source
      and (
        (a.hosted_sha256 is not null and a.hosted_sha256 = s.hosted_sha256)
        or (a.source_asset_id is not null and a.source_asset_id = s.source_image_id)
        or (a.source_url is not null and a.source_url = coalesce(s.url_original, s.url))
      )
    order by
      (a.hosted_sha256 is not null and a.hosted_sha256 = s.hosted_sha256) desc,
      (a.source_asset_id is not null and a.source_asset_id = s.source_image_id) desc,
      (a.source_url is not null and a.source_url = coalesce(s.url_original, s.url)) desc,
      a.id
    limit 1
  ) a on true
  where a.id is null
     or not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'season'
      and ml.entity_id = s.season_id
      and ml.kind = s.kind
      and ml.media_asset_id = a.id
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
  left join lateral (
    select a.id
    from core.media_assets a
    where a.source = e.source
      and (
        (a.hosted_sha256 is not null and a.hosted_sha256 = e.hosted_sha256)
        or (a.source_asset_id is not null and a.source_asset_id = e.source_image_id)
        or (a.source_url is not null and a.source_url = coalesce(e.url_original, e.url))
      )
    order by
      (a.hosted_sha256 is not null and a.hosted_sha256 = e.hosted_sha256) desc,
      (a.source_asset_id is not null and a.source_asset_id = e.source_image_id) desc,
      (a.source_url is not null and a.source_url = coalesce(e.url_original, e.url)) desc,
      a.id
    limit 1
  ) a on true
  where a.id is null
     or not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'episode'
      and ml.entity_id = e.episode_id
      and ml.kind = e.kind
      and ml.media_asset_id = a.id
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
  left join lateral (
    select a.id
    from core.media_assets a
    where a.source = p.source
      and a.source_url = p.url
    order by a.id
    limit 1
  ) a on true
  where a.id is null
     or not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'person'
      and ml.kind = 'profile'
      and ml.media_asset_id = a.id
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
  left join lateral (
    select a.id
    from core.media_assets a
    where a.source = c.source
      and (
        (a.hosted_sha256 is not null and a.hosted_sha256 = c.hosted_sha256)
        or (a.source_asset_id is not null and a.source_asset_id = c.source_image_id)
        or (a.source_url is not null and a.source_url = coalesce(c.image_url_canonical, c.image_url, c.url, c.thumb_url))
      )
    order by
      (a.hosted_sha256 is not null and a.hosted_sha256 = c.hosted_sha256) desc,
      (a.source_asset_id is not null and a.source_asset_id = c.source_image_id) desc,
      (a.source_url is not null and a.source_url = coalesce(c.image_url_canonical, c.image_url, c.url, c.thumb_url)) desc,
      a.id
    limit 1
  ) a on true
  where a.id is null
     or not exists (
    select 1
    from core.media_links ml
    where ml.entity_type = 'person'
      and ml.kind = 'gallery'
      and ml.media_asset_id = a.id
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
