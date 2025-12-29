# core.show_cast

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| show_name | text | YES |  | NO | NEVER |
| cast_member_name | text | YES |  | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| billing_order | integer | YES |  | NO | NEVER |
| role | text | YES |  | NO | NEVER |
| credit_category | text | NO | 'Self'::text | NO | NEVER |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- show_id, person_id, credit_category

## Foreign Keys

- person_id -> core.people.id
- show_id -> core.shows.id

## Indexes

- core_show_cast_person_id_idx (non-unique): person_id
- core_show_cast_show_id_idx (non-unique): show_id
- show_cast_pkey1 (unique): id
- show_cast_show_id_person_id_credit_category_key (unique): show_id, person_id, credit_category

## RLS Enabled

true

## Example Row

```json
{
  "show_name": "example",
  "cast_member_name": "example",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "billing_order": 0,
  "role": "example",
  "credit_category": "example",
  "id": "00000000-0000-0000-0000-000000000000",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```