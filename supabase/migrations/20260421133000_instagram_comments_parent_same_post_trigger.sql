begin;

create or replace function social.enforce_instagram_comment_parent_same_post()
returns trigger as $$
declare
  parent_post_id uuid;
begin
  if new.parent_comment_id is null then
    return new;
  end if;

  select post_id into parent_post_id
  from social.instagram_comments
  where id = new.parent_comment_id;

  if parent_post_id is null then
    raise exception 'parent_comment_id % not found', new.parent_comment_id;
  end if;

  if parent_post_id <> new.post_id then
    raise exception 'parent_comment post_id (%) does not match child post_id (%)', parent_post_id, new.post_id;
  end if;

  return new;
end;
$$ language plpgsql;

drop trigger if exists instagram_comments_parent_same_post_tg on social.instagram_comments;

create trigger instagram_comments_parent_same_post_tg
before insert or update of parent_comment_id, post_id
on social.instagram_comments
for each row
execute function social.enforce_instagram_comment_parent_same_post();

commit;
