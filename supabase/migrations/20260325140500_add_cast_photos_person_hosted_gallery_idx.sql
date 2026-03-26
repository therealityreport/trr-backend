create index if not exists cast_photos_person_hosted_gallery_idx
on core.cast_photos (person_id, gallery_index)
where hosted_url is not null;
