# core.credits

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| credit_category | text | NO |  | NO | NEVER |
| role | text | YES |  | NO | NEVER |
| billing_order | integer | YES |  | NO | NEVER |
| source_type | text | NO |  | NO | NEVER |
| metadata | jsonb | NO | '{}'::jsonb | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- person_id -> core.people.id
- show_id -> core.shows.id

## Indexes

- credits_person_id_idx (non-unique): person_id
- credits_pkey (unique): id
- credits_show_id_category_idx (non-unique): show_id, credit_category
- credits_show_id_idx (non-unique): show_id
- credits_source_type_idx (non-unique): source_type
- credits_unique_idx (unique): show_id, person_id, credit_category, COALESCE(role, ''::text), source_type

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "credit_category": "example",
  "role": "example",
  "billing_order": 0,
  "source_type": "example",
  "metadata": {},
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```