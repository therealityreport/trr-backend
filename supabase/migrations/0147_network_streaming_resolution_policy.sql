begin;

alter table if exists admin.network_streaming_completion
  add column if not exists resolution_policy text not null default 'strict',
  add column if not exists logo_required boolean not null default true;

alter table if exists admin.network_streaming_completion
  drop constraint if exists network_streaming_completion_resolution_policy_check;

alter table if exists admin.network_streaming_completion
  add constraint network_streaming_completion_resolution_policy_check
  check (resolution_policy in ('strict', 'production_logo_optional'));

update admin.network_streaming_completion
set
  resolution_policy = 'production_logo_optional',
  logo_required = false
where entity_type = 'production';

update admin.network_streaming_completion
set
  resolution_policy = 'strict',
  logo_required = true
where entity_type in ('network', 'streaming');

commit;
