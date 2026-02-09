-- Migration: Add screenalytics facebank seed/exemplar images table
-- This table is owned by Screenalytics, but lives in Supabase under screenalytics.*

begin;

create schema if not exists screenalytics;

create table screenalytics.face_bank_images (
  image_id uuid primary key default gen_random_uuid(),
  person_id uuid not null references core.people (id) on delete cascade,
  media_asset_id uuid references core.media_assets (id) on delete set null,
  s3_original_key text not null,
  s3_aligned_key text,
  s3_embedding_key text,
  quality_score double precision,
  is_seed boolean not null default false,
  approved boolean not null default false,
  approved_at timestamptz,
  approved_by text,
  created_at timestamptz not null default now()
);

create index idx_sa_face_bank_images_person_id on screenalytics.face_bank_images (person_id);
create index idx_sa_face_bank_images_seed on screenalytics.face_bank_images (person_id, is_seed)
  where is_seed = true;

-- Grants (service_role only)
grant usage on schema screenalytics to service_role;
grant all privileges on table screenalytics.face_bank_images to service_role;

-- RLS with explicit service_role policies
alter table screenalytics.face_bank_images enable row level security;

create policy "service_role_all_face_bank_images"
on screenalytics.face_bank_images for all to service_role
using (true) with check (true);

commit;

