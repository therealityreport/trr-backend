begin;

revoke select on table core.cast_fandom from anon, authenticated;
revoke select on table core.cast_photos from anon, authenticated;

commit;
