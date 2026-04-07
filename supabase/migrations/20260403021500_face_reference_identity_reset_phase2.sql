begin;

alter table ml.face_reference_images
  add column if not exists legacy_screenalytics_face_bank_image_id uuid,
  add column if not exists review_status text not null default 'pending_review',
  add column if not exists review_notes jsonb not null default '{}'::jsonb,
  add column if not exists reviewed_at timestamptz,
  add column if not exists reviewed_by text,
  add column if not exists duplicate_of_reference_image_id uuid references ml.face_reference_images (id) on delete set null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ml_face_reference_images_review_status_check'
      and conrelid = 'ml.face_reference_images'::regclass
  ) then
    alter table ml.face_reference_images
      add constraint ml_face_reference_images_review_status_check
      check (review_status in ('pending_review', 'approved', 'rejected', 'duplicate'));
  end if;
end $$;

create unique index if not exists ml_face_reference_images_legacy_face_bank_image_idx
  on ml.face_reference_images (legacy_screenalytics_face_bank_image_id)
  where legacy_screenalytics_face_bank_image_id is not null;

create index if not exists ml_face_reference_images_review_state_idx
  on ml.face_reference_images (person_id, review_status, is_active);

create index if not exists ml_face_reference_embeddings_contract_status_idx
  on ml.face_reference_embeddings (provider, model_name, embedding_status);

create index if not exists ml_face_reference_embeddings_embedding_hnsw_idx
  on ml.face_reference_embeddings using hnsw (embedding vector_cosine_ops)
  where embedding is not null and embedding_status = 'ready';

update ml.face_reference_images
set review_status = case
      when approved = true then 'approved'
      else 'pending_review'
    end,
    review_notes = coalesce(review_notes, '{}'::jsonb),
    reviewed_at = case
      when approved = true and reviewed_at is null then created_at
      else reviewed_at
    end,
    reviewed_by = case
      when approved = true and nullif(trim(reviewed_by), '') is null then 'phase2-backfill'
      else reviewed_by
    end
where review_status is distinct from case
        when approved = true then 'approved'
        else 'pending_review'
      end
   or review_notes is null
   or (approved = true and reviewed_at is null)
   or (approved = true and nullif(trim(reviewed_by), '') is null);

do $$
begin
  if to_regclass('screenalytics.face_bank_images') is not null then
    with matched_legacy as (
      select
        fbi.image_id,
        fbi.person_id,
        fbi.media_asset_id,
        fbi.approved,
        fbi.approved_at,
        fbi.approved_by,
        fbi.s3_original_key,
        fbi.s3_aligned_key,
        fbi.s3_embedding_key,
        fbi.quality_score,
        ml.id as media_link_id,
        ma.source_url,
        ma.hosted_url,
        ma.hosted_sha256,
        count(*) over (partition by fbi.image_id) as match_count,
        row_number() over (partition by fbi.image_id order by ml.id) as row_num
      from screenalytics.face_bank_images fbi
      join core.media_links ml
        on ml.entity_type = 'person'
       and ml.kind = 'gallery'
       and ml.entity_id = fbi.person_id
       and ml.media_asset_id = fbi.media_asset_id
      left join core.media_assets ma on ma.id = fbi.media_asset_id
      where fbi.is_seed = true
        and fbi.media_asset_id is not null
    )
    insert into ml.face_reference_images (
      person_id,
      media_link_id,
      media_asset_id,
      legacy_screenalytics_face_bank_image_id,
      is_active,
      approved,
      review_status,
      review_notes,
      reviewed_at,
      reviewed_by,
      embedding_status,
      source_url,
      hosted_url,
      hosted_sha256,
      metadata
    )
    select
      matched_legacy.person_id,
      matched_legacy.media_link_id,
      matched_legacy.media_asset_id,
      matched_legacy.image_id,
      true,
      matched_legacy.approved,
      case
        when matched_legacy.approved then 'approved'
        else 'pending_review'
      end,
      jsonb_build_object(
        'source', 'screenalytics.face_bank_images',
        'legacy', true,
        'quality_score', matched_legacy.quality_score,
        's3_original_key', matched_legacy.s3_original_key,
        's3_aligned_key', matched_legacy.s3_aligned_key,
        's3_embedding_key', matched_legacy.s3_embedding_key
      ),
      case
        when matched_legacy.approved then coalesce(matched_legacy.approved_at, now())
        else null
      end,
      case
        when matched_legacy.approved then coalesce(nullif(trim(matched_legacy.approved_by), ''), 'screenalytics')
        else null
      end,
      'pending',
      matched_legacy.source_url,
      matched_legacy.hosted_url,
      matched_legacy.hosted_sha256,
      jsonb_build_object(
        'source', 'screenalytics.face_bank_images',
        'legacy_face_bank_image_id', matched_legacy.image_id,
        'quality_score', matched_legacy.quality_score,
        's3_original_key', matched_legacy.s3_original_key,
        's3_aligned_key', matched_legacy.s3_aligned_key,
        's3_embedding_key', matched_legacy.s3_embedding_key
      )
    from matched_legacy
    where matched_legacy.match_count = 1
      and matched_legacy.row_num = 1
    on conflict (media_link_id) do update
      set legacy_screenalytics_face_bank_image_id =
            coalesce(
              ml.face_reference_images.legacy_screenalytics_face_bank_image_id,
              excluded.legacy_screenalytics_face_bank_image_id
            ),
          review_notes = ml.face_reference_images.review_notes || excluded.review_notes,
          metadata = ml.face_reference_images.metadata || excluded.metadata,
          updated_at = now();
  end if;
end $$;

commit;
