begin;

grant all privileges on table core.media_assets to service_role;
grant all privileges on table core.media_links to service_role;

commit;
