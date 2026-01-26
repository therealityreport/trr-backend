begin;

with ranked as (
  select
    ml.entity_type,
    ml.entity_id,
    ml.kind,
    ml.id as media_link_id,
    row_number() over (
      partition by ml.entity_type, ml.entity_id, ml.kind
      order by
        (ma.hosted_url is not null) desc,
        (coalesce(ma.width,0) * coalesce(ma.height,0)) desc,
        coalesce(ml.position, 9999) asc,
        ma.created_at asc
    ) as rn
  from core.media_links ml
  join core.media_assets ma on ma.id = ml.media_asset_id
  where (ml.context->>'legacy_table') is not null
)
update core.media_links ml
set is_primary = (r.rn = 1)
from ranked r
where ml.id = r.media_link_id;

-- Gate: fail if duplicates remain

do $$
begin
  if exists (
    select 1
    from core.media_links
    where is_primary = true
    group by entity_type, entity_id, kind
    having count(*) > 1
  ) then
    raise exception 'Primary media constraint violated';
  end if;
end $$;

commit;
