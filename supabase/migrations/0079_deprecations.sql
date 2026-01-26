begin;

-- Only after read paths switch and validation gates pass.
-- Examples:
-- alter table core.shows drop column primary_poster_image_id;
-- alter table core.shows drop column primary_backdrop_image_id;
-- alter table core.shows drop column primary_logo_image_id;
-- alter table core.shows drop column external_ids;

-- Drop or rename legacy tables (or replace with views):
-- drop table core.show_images;
-- drop table core.season_images;
-- drop table core.episode_images;
-- drop table core.person_images;
-- drop table core.cast_photos;

commit;
