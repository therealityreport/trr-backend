begin;

alter table social.instagram_comments
  add column if not exists hosted_author_profile_pic_url text;

commit;
