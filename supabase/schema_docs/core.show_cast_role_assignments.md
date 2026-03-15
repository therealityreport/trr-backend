# core.show_cast_role_assignments

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| season_id | uuid | YES |  | NO | NEVER |
| season_number | integer | NO | 0 | NO | NEVER |
| role_id | uuid | NO |  | NO | NEVER |
| source | text | NO | 'manual'::text | NO | NEVER |
| confidence | numeric | YES |  | NO | NEVER |
| metadata | jsonb | NO | '{}'::jsonb | NO | NEVER |
| created_by | text | YES |  | NO | NEVER |
| updated_by | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- show_id, person_id, season_number, role_id

## Foreign Keys

- person_id -> core.people.id
- role_id -> core.show_role_catalog.id
- season_id -> core.seasons.id
- show_id -> core.shows.id

## Indexes

- show_cast_role_assignments_pkey (unique): id
- show_cast_role_assignments_role_idx (non-unique): role_id
- show_cast_role_assignments_show_person_idx (non-unique): show_id, person_id, season_number
- show_cast_role_assignments_unique (unique): show_id, person_id, season_number, role_id

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "season_id": "00000000-0000-0000-0000-000000000000",
  "season_number": 0,
  "role_id": "00000000-0000-0000-0000-000000000000",
  "source": "example",
  "confidence": 0,
  "metadata": {},
  "created_by": "example",
  "updated_by": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```