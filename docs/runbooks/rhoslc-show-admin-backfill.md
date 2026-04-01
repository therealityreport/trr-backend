# RHOSLC Show Admin Backfill Runbook

Last updated: February 12, 2026
Owner: TRR-Backend

## Goal

Backfill RHOSLC with:
- discovered `entity_links` (official/social/knowledge/cast announcements)
- cast-role suggestions from Bravo cast-announcement content (`show_cast_role_assignments`, source=`bravo_cast_announcement`)

This runbook uses the new admin endpoints added in the Show Admin overhaul.

## Prerequisites

1. Backend is running (`http://127.0.0.1:8000`).
2. You have an admin bearer token.
3. `TRR_DB_URL` is set for verification queries (or `TRR_DB_FALLBACK_URL` for an explicit fallback).

```bash
export TRR_BACKEND_URL="http://127.0.0.1:8000"
export ADMIN_BEARER_TOKEN="<allowlisted user JWT or internal admin JWT>"
export SHOW_SLUG_URL="https://www.bravotv.com/the-real-housewives-of-salt-lake-city"
```

Helper headers:

```bash
AUTH=(-H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" -H "Content-Type: application/json")
```

## 1) Resolve RHOSLC show id + seasons

```bash
export RHOSLC_SHOW_ID="$(psql "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}" -Atc "
  select id
  from core.shows
  where lower(name) = 'the real housewives of salt lake city'
  limit 1;
")"

echo "RHOSLC show id: ${RHOSLC_SHOW_ID}"

psql "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}" -c "
  select season_number, id
  from core.seasons
  where show_id = '${RHOSLC_SHOW_ID}'::uuid
  order by season_number;
"
```

## 2) Preview Bravo payload by season (optional but recommended)

```bash
for SEASON in 1 2 3 4 5 6; do
  echo "--- Preview season ${SEASON} ---"
  curl -sS "${TRR_BACKEND_URL}/api/v1/admin/shows/${RHOSLC_SHOW_ID}/import-bravo/preview" \
    "${AUTH[@]}" \
    -X POST \
    -d "{\"show_url\":\"${SHOW_SLUG_URL}\",\"season_number\":${SEASON},\"include_people\":true,\"include_news\":true,\"include_videos\":true}" \
  | jq '{season:'"${SEASON}"', people:(.people|length), news:(.news|length), videos:(.videos|length), discovered_person_urls:(.discovered_person_urls|length)}'
done
```

## 3) Execute Bravo commit by season (writes links + role suggestions)

```bash
for SEASON in 1 2 3 4 5 6; do
  echo "--- Commit season ${SEASON} ---"
  curl -sS "${TRR_BACKEND_URL}/api/v1/admin/shows/${RHOSLC_SHOW_ID}/import-bravo/commit" \
    "${AUTH[@]}" \
    -X POST \
    -d "{\"show_url\":\"${SHOW_SLUG_URL}\",\"season_number\":${SEASON}}" \
  | jq '{season:'"${SEASON}"', discovered_links:(.counts.discovered_links // 0), role_suggestions:(.counts.role_suggestions // 0), role_assignments:(.counts.role_assignments // 0), announcement_people:(.counts.announcement_people // 0), unmatched_people:(.counts.unmatched_people // 0)}'
done
```

Notes:
- Season-scoped commits update `core.seasons.overview` for that season and do **not** overwrite global `core.shows.description`.
- If `unmatched_people > 0`, rerun commit with `person_url_mappings` for unresolved Bravo person URLs.

## 4) Run explicit link discovery pass (show + seasons + people)

```bash
curl -sS "${TRR_BACKEND_URL}/api/v1/admin/shows/${RHOSLC_SHOW_ID}/links/discover" \
  "${AUTH[@]}" \
  -X POST \
  -d '{"include_seasons":true,"include_people":true}' \
| jq
```

## 5) Review pending links (admin review gate)

List pending links:

```bash
curl -sS "${TRR_BACKEND_URL}/api/v1/admin/shows/${RHOSLC_SHOW_ID}/links?status=pending" \
  -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" \
| jq '.[] | {id, entity_type, season_number, link_group, link_kind, label, url, confidence, source, discovered_by}'
```

Example approve one link:

```bash
export LINK_ID="<entity_link_uuid>"
curl -sS "${TRR_BACKEND_URL}/api/v1/admin/shows/${RHOSLC_SHOW_ID}/links/${LINK_ID}" \
  "${AUTH[@]}" \
  -X PATCH \
  -d '{"status":"approved"}' \
| jq
```

Example reject one link:

```bash
curl -sS "${TRR_BACKEND_URL}/api/v1/admin/shows/${RHOSLC_SHOW_ID}/links/${LINK_ID}" \
  "${AUTH[@]}" \
  -X PATCH \
  -d '{"status":"rejected"}' \
| jq
```

## 6) Verify role suggestions persisted

API check:

```bash
curl -sS "${TRR_BACKEND_URL}/api/v1/admin/shows/${RHOSLC_SHOW_ID}/cast-role-members?sort_by=season&order=desc" \
  -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" \
| jq '.[0:25]'
```

DB checks:

```bash
psql "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}" -c "
  select
    sra.season_number,
    rc.name as role_name,
    count(*) as assignments
  from core.show_cast_role_assignments sra
  join core.show_role_catalog rc on rc.id = sra.role_id
  where sra.show_id = '${RHOSLC_SHOW_ID}'::uuid
    and sra.source = 'bravo_cast_announcement'
  group by sra.season_number, rc.name
  order by sra.season_number desc, rc.name;
"

psql "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}" -c "
  select
    status,
    link_group,
    count(*)
  from core.entity_links
  where show_id = '${RHOSLC_SHOW_ID}'::uuid
  group by status, link_group
  order by status, link_group;
"
```

## 7) Completion checklist

- Bravo season commits completed for RHOSLC seasons in scope.
- `discovered_links` and `role_suggestions` counts are non-zero where expected.
- Pending links reviewed (`approved` / `rejected`) by admin.
- `show_cast_role_assignments` contain `bravo_cast_announcement` rows.
