-- Schema-only rollback for 20260713150000_instagram_current_payload_sidecars.sql.
-- Do not run during the compare/cutover rollback window: flipping reads to
-- legacy is the operational rollback and keeps these private rows available.
begin;
drop table if exists social.instagram_account_catalog_post_payloads;
drop table if exists social.instagram_post_payloads;
commit;

