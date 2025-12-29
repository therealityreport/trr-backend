# core.cast_memberships

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| season_id | uuid | YES |  | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| role | text | NO | 'cast'::text | NO | NEVER |
| billing_order | integer | YES |  | NO | NEVER |
| notes | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- person_id -> core.people.id
- season_id -> core.seasons.id
- show_id -> core.shows.id

## Indexes

- cast_memberships_person_id_idx (non-unique): person_id
- cast_memberships_pkey (unique): id
- cast_memberships_season_id_idx (non-unique): season_id
- cast_memberships_show_id_idx (non-unique): show_id
- cast_memberships_show_person_role_no_season_unique_idx (unique): show_id, person_id, role) WHERE (season_id IS NULL
- cast_memberships_show_season_person_role_unique_idx (unique): show_id, season_id, person_id, role) WHERE (season_id IS NOT NULL

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "season_id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "role": "example",
  "billing_order": 0,
  "notes": "example",
  "created_at": "1970-01-01T00:00:00Z"
}
```