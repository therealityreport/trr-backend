# core.people_overrides

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| full_name_override | text | YES |  | NO | NEVER |
| instagram_handle | text | YES |  | NO | NEVER |
| external_ids_override | jsonb | NO | '{}'::jsonb | NO | NEVER |
| notes | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| tiktok_handle | text | YES |  | NO | NEVER |
| twitter_handle | text | YES |  | NO | NEVER |
| youtube_handle | text | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

- person_id

## Foreign Keys

- person_id -> core.people.id

## Indexes

- idx_people_overrides_person_id (non-unique): person_id
- people_overrides_person_id_key (unique): person_id
- people_overrides_pkey (unique): id

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "full_name_override": "example",
  "instagram_handle": "example",
  "external_ids_override": {},
  "notes": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "tiktok_handle": "example",
  "twitter_handle": "example",
  "youtube_handle": "example"
}
```