# core.people

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| full_name | text | NO |  | NO | NEVER |
| known_for | text | YES |  | NO | NEVER |
| external_ids | jsonb | NO | '{}'::jsonb | NO | NEVER |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- core_people_imdb_unique (unique): ((external_ids ->> 'imdb'::text))) WHERE ((external_ids ? 'imdb'::text) AND (btrim((external_ids ->> 'imdb'::text)) <> ''::text)
- people_full_name_idx (non-unique): full_name
- people_pkey1 (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "full_name": "example",
  "known_for": "example",
  "external_ids": {},
  "id": "00000000-0000-0000-0000-000000000000",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```