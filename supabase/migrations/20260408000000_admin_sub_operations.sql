-- Sub-operation support: link child operations to a parent for target-level
-- dispatch and aggregation in show refresh (and future multi-step workflows).

alter table core.admin_operations
  add column if not exists parent_operation_id uuid
    references core.admin_operations(id) on delete cascade,
  add column if not exists refresh_target text;

comment on column core.admin_operations.parent_operation_id is
  'FK to parent operation. Null for top-level operations.';
comment on column core.admin_operations.refresh_target is
  'Refresh target key (show_core, links, bravo, cast_profiles, cast_media) for sub-operations.';

create index if not exists idx_admin_operations_parent_id
  on core.admin_operations(parent_operation_id, created_at)
  where parent_operation_id is not null;

create index if not exists idx_admin_operations_parent_status
  on core.admin_operations(parent_operation_id, status)
  where parent_operation_id is not null;
