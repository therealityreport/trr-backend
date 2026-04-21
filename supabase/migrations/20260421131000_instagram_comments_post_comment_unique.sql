begin;

alter table social.instagram_comments
  drop constraint if exists instagram_comments_comment_id_key;

alter table social.instagram_comments
  add constraint instagram_comments_post_comment_unique unique (post_id, comment_id);

commit;
