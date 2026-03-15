# core.show_cast_overrides

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| credit_category | text | NO | 'Self'::text | NO | NEVER |
| friend_of | boolean | YES |  | NO | NEVER |
| role_override | text | YES |  | NO | NEVER |
| billing_order_override | integer | YES |  | NO | NEVER |
| notes_override | text | YES |  | NO | NEVER |
| tags_override | ARRAY | YES |  | NO | NEVER |
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

- idx_show_cast_overrides_person_id (non-unique): person_id
- idx_show_cast_overrides_show_id (non-unique): show_id
- show_cast_overrides_pkey (unique): id
- show_cast_overrides_show_id_person_id_credit_category_key (unique): show_id, person_id, credit_category

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "credit_category": "example",
  "friend_of": false,
  "role_override": "example",
  "billing_order_override": 0,
  "notes_override": "example",
  "tags_override": [],
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```