-- Verify media unification bridges and coverage.
-- Fails fast with exceptions if checks do not pass.

-- Bridge triggers exist and are enabled on expected tables.
do $$
declare
  trigger_names text[] := array[
    'bridge_show_images_to_media',
    'bridge_season_images_to_media',
    'bridge_episode_images_to_media',
    'bridge_person_images_to_media',
    'bridge_cast_photos_to_media',
    'bridge_show_source_snapshots'
  ];
  trigger_tables text[] := array[
    'core.show_images',
    'core.season_images',
    'core.episode_images',
    'core.person_images',
    'core.cast_photos',
    'core.shows'
  ];
  i int;
  actual_table text;
  enabled text;
begin
  for i in 1..array_length(trigger_names, 1) loop
    select tgrelid::regclass::text, tgenabled
      into actual_table, enabled
    from pg_trigger
    where tgname = trigger_names[i];

    if actual_table is null then
      raise exception 'Missing trigger %', trigger_names[i];
    end if;

    if actual_table <> trigger_tables[i] then
      raise exception 'Trigger % attached to %, expected %', trigger_names[i], actual_table, trigger_tables[i];
    end if;

    if enabled not in ('O','A') then
      raise exception 'Trigger % is disabled (tgenabled=%)', trigger_names[i], enabled;
    end if;
  end loop;
end $$;

-- Coverage: legacy rows have mirrored media_links via legacy_id.
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
      and (ml.context->>'legacy_table') = 'show_images'
      and (ml.context->>'legacy_id')::uuid = s.id
  );
  if missing > 0 then
    raise exception 'Missing media_links for show_images rows: %', missing;
  end if;
end $$;

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
      and (ml.context->>'legacy_table') = 'season_images'
      and (ml.context->>'legacy_id')::uuid = s.id
  );
  if missing > 0 then
    raise exception 'Missing media_links for season_images rows: %', missing;
  end if;
end $$;

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
      and (ml.context->>'legacy_table') = 'episode_images'
      and (ml.context->>'legacy_id')::uuid = e.id
  );
  if missing > 0 then
    raise exception 'Missing media_links for episode_images rows: %', missing;
  end if;
end $$;

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
    raise exception 'Missing media_links for person_images rows: %', missing;
  end if;
end $$;

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
    raise exception 'Missing media_links for cast_photos rows: %', missing;
  end if;
end $$;

-- Primary uniqueness per entity/kind.
do $$
begin
  if exists (
    select 1
    from core.media_links
    where is_primary = true
    group by entity_type, entity_id, kind
    having count(*) > 1
  ) then
    raise exception 'Duplicate primary media_links detected';
  end if;
end $$;

-- Optional snapshot bridge check (manual):
-- Update a show tmdb_meta and confirm latest/history rows exist.
-- Uncomment to run interactively.
-- do $$
-- declare
--   v_show_id uuid;
--   latest_count int;
--   history_count int;
-- begin
--   select id into v_show_id from core.shows order by created_at desc limit 1;
--   if v_show_id is null then
--     raise exception 'No shows found to validate snapshot bridge';
--   end if;
--
--   update core.shows
--   set tmdb_meta = jsonb_build_object('verify_media_bridge', true, 'ts', now()::text),
--       tmdb_fetched_at = now()
--   where id = v_show_id;
--
--   select count(*) into latest_count
--   from core.show_source_latest
--   where show_id = v_show_id
--     and source_id = 'tmdb'
--     and variant = 'details'
--     and payload->>'verify_media_bridge' = 'true';
--
--   select count(*) into history_count
--   from core.show_source_history
--   where show_id = v_show_id
--     and source_id = 'tmdb'
--     and variant = 'details'
--     and payload->>'verify_media_bridge' = 'true';
--
--   if latest_count < 1 then
--     raise exception 'Snapshot bridge latest check failed';
--   end if;
--   if history_count < 1 then
--     raise exception 'Snapshot bridge history check failed';
--   end if;
-- end $$;
