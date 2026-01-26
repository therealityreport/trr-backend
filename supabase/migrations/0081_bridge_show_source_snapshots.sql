begin;

create schema if not exists core;

create or replace function core.bridge_show_source_snapshots()
returns trigger
language plpgsql
as $$
declare
  v_payload_sha text;
  v_payload_sha_imdb text;
begin
  if tg_op = 'DELETE' then
    return old;
  end if;

  if new.tmdb_meta is not null and (tg_op = 'INSERT' or new.tmdb_meta is distinct from old.tmdb_meta or new.tmdb_fetched_at is distinct from old.tmdb_fetched_at) then
    v_payload_sha := core.jsonb_sha256(new.tmdb_meta);

    insert into core.show_source_latest(
      show_id, source_id, variant, fetched_at, fetch_method, status, payload, payload_sha256
    ) values (
      new.id, 'tmdb', 'details', coalesce(new.tmdb_fetched_at, now()), 'legacy_column', 'success',
      new.tmdb_meta, v_payload_sha
    )
    on conflict (show_id, source_id, variant) do update
    set fetched_at = excluded.fetched_at,
        fetch_method = excluded.fetch_method,
        status = excluded.status,
        payload = excluded.payload,
        payload_sha256 = excluded.payload_sha256;

    insert into core.show_source_history(
      show_id, source_id, variant, fetched_at, fetch_method, status, payload, payload_sha256
    )
    select new.id, 'tmdb', 'details', coalesce(new.tmdb_fetched_at, now()), 'legacy_column', 'success',
           new.tmdb_meta, v_payload_sha
    where not exists (
      select 1 from core.show_source_history h
      where h.show_id = new.id and h.source_id = 'tmdb' and h.variant = 'details'
        and h.payload_sha256 = v_payload_sha
    );
  end if;

  if new.imdb_meta is not null and (tg_op = 'INSERT' or new.imdb_meta is distinct from old.imdb_meta or new.imdb_fetched_at is distinct from old.imdb_fetched_at) then
    v_payload_sha_imdb := core.jsonb_sha256(new.imdb_meta);

    insert into core.show_source_latest(
      show_id, source_id, variant, fetched_at, fetch_method, status, payload, payload_sha256
    ) values (
      new.id, 'imdb', 'details', coalesce(new.imdb_fetched_at, now()), 'legacy_column', 'success',
      new.imdb_meta, v_payload_sha_imdb
    )
    on conflict (show_id, source_id, variant) do update
    set fetched_at = excluded.fetched_at,
        fetch_method = excluded.fetch_method,
        status = excluded.status,
        payload = excluded.payload,
        payload_sha256 = excluded.payload_sha256;

    insert into core.show_source_history(
      show_id, source_id, variant, fetched_at, fetch_method, status, payload, payload_sha256
    )
    select new.id, 'imdb', 'details', coalesce(new.imdb_fetched_at, now()), 'legacy_column', 'success',
           new.imdb_meta, v_payload_sha_imdb
    where not exists (
      select 1 from core.show_source_history h
      where h.show_id = new.id and h.source_id = 'imdb' and h.variant = 'details'
        and h.payload_sha256 = v_payload_sha_imdb
    );
  end if;

  return new;
end $$;

-- Trigger guard

do $$
begin
  if exists (select 1 from pg_trigger where tgname = 'bridge_show_source_snapshots') then
    drop trigger bridge_show_source_snapshots on core.shows;
  end if;
  create trigger bridge_show_source_snapshots
  after insert or update of tmdb_meta, tmdb_fetched_at, imdb_meta, imdb_fetched_at on core.shows
  for each row execute function core.bridge_show_source_snapshots();
end $$;

commit;
