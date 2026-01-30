-- Create admin schema and image audit log table
begin;

-- Create admin schema if it doesn't exist
create schema if not exists admin;

-- Create audit log table for image management actions
create table if not exists admin.image_audit_log (
  id uuid primary key default gen_random_uuid(),
  image_type text not null check (image_type in ('cast', 'episode', 'season')),
  image_id uuid not null,
  action text not null check (action in ('archive', 'unarchive', 'delete', 'reassign', 'copy_reassign')),
  performed_by_firebase_uid text not null,
  performed_at timestamptz not null default now(),
  details jsonb
);

-- Indexes for common query patterns
create index if not exists idx_image_audit_log_image
  on admin.image_audit_log (image_type, image_id);

create index if not exists idx_image_audit_log_performed_at
  on admin.image_audit_log (performed_at desc);

-- Grants: admin table only accessible by service_role
grant usage on schema admin to service_role;
grant all privileges on table admin.image_audit_log to service_role;

commit;
