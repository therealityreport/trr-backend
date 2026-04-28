-- Security Advisor hotfix gate for public migration ledger and exposed
-- SECURITY DEFINER RPC execution. This is intentionally separate from the
-- performance policy cleanup migration.

do $$
begin
  if to_regclass('public.__migrations') is not null then
    execute 'alter table public.__migrations enable row level security';
    execute 'revoke all on table public.__migrations from public';
    execute 'revoke all on table public.__migrations from anon';
    execute 'revoke all on table public.__migrations from authenticated';
    execute 'drop policy if exists "__migrations_no_api_access" on public.__migrations';
    execute $ddl$
      create policy "__migrations_no_api_access"
      on public.__migrations
      as restrictive
      for all
      to public
      using (false)
      with check (false)
    $ddl$;
    execute $ddl$
      comment on policy "__migrations_no_api_access" on public.__migrations is
        'Deny API-role access to the app migration ledger. Owner/migration connections retain direct DB access.'
    $ddl$;
  end if;
end;
$$;

revoke execute on function core.merge_shows(uuid, uuid) from public, anon, authenticated;
revoke execute on function core.set_primary_media_link(text, uuid, text, uuid) from public, anon, authenticated;
revoke execute on function core.upsert_cast_photos_by_canonical(jsonb) from public, anon, authenticated;
revoke execute on function core.upsert_cast_photos_by_identity(jsonb) from public, anon, authenticated;
revoke execute on function core.upsert_person_images(jsonb) from public, anon, authenticated;
revoke execute on function core.upsert_show_images_by_identity(jsonb) from public, anon, authenticated;
revoke execute on function core.upsert_tmdb_show_images_by_identity(jsonb) from public, anon, authenticated;
revoke execute on function social.get_or_create_direct_conversation(uuid) from public, anon, authenticated;

grant execute on function core.merge_shows(uuid, uuid) to service_role;
grant execute on function core.set_primary_media_link(text, uuid, text, uuid) to service_role;
grant execute on function core.upsert_cast_photos_by_canonical(jsonb) to service_role;
grant execute on function core.upsert_cast_photos_by_identity(jsonb) to service_role;
grant execute on function core.upsert_person_images(jsonb) to service_role;
grant execute on function core.upsert_show_images_by_identity(jsonb) to service_role;
grant execute on function core.upsert_tmdb_show_images_by_identity(jsonb) to service_role;
grant execute on function social.get_or_create_direct_conversation(uuid) to service_role;

comment on function social.get_or_create_direct_conversation(uuid) is
  'Creates or retrieves a 1:1 DM conversation. Execute was revoked from public, anon, and authenticated by 20260428110000; expose through a backend-owned path before enabling client access again.';
