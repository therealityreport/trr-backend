begin;

-- ---------------------------------------------------------------------------
-- Atomic RPC for setting primary media link
-- ---------------------------------------------------------------------------
-- This function atomically sets a media link as primary for an entity+kind,
-- unsetting any previous primary in the same transaction.
--
-- Includes validation to prevent setting a link that doesn't belong to the
-- specified entity/kind, and row-level locking to prevent race conditions.
-- ---------------------------------------------------------------------------

create or replace function core.set_primary_media_link(
  p_entity_type text,
  p_entity_id uuid,
  p_kind text,
  p_media_link_id uuid
) returns void as $$
declare
  v_affected int;
begin
  -- Lock rows for this entity+kind to prevent races
  perform 1 from core.media_links
  where entity_type = p_entity_type
    and entity_id = p_entity_id
    and kind = p_kind
  for update;

  -- Unset all existing primaries for this entity+kind
  update core.media_links
  set is_primary = false, updated_at = now()
  where entity_type = p_entity_type
    and entity_id = p_entity_id
    and kind = p_kind
    and is_primary = true;

  -- Set the requested link as primary (ONLY if it belongs to this entity+kind)
  update core.media_links
  set is_primary = true, updated_at = now()
  where id = p_media_link_id
    and entity_type = p_entity_type
    and entity_id = p_entity_id
    and kind = p_kind;

  get diagnostics v_affected = row_count;
  if v_affected = 0 then
    raise exception 'Media link % does not belong to entity (%, %, %)',
      p_media_link_id, p_entity_type, p_entity_id, p_kind;
  end if;
end;
$$ language plpgsql security definer;

-- Only service_role can call this function (admin-only for v1)
grant execute on function core.set_primary_media_link(text, uuid, text, uuid) to service_role;

commit;
