alter table if exists admin.brand_logo_source_queries
  add column if not exists query_values jsonb not null default '[]'::jsonb;

update admin.brand_logo_source_queries
set query_values = case
  when coalesce(trim(query_value), '') = '' then '[]'::jsonb
  else jsonb_build_array(trim(query_value))
end
where query_values = '[]'::jsonb;

grant all privileges on table admin.brand_logo_source_queries to service_role;
