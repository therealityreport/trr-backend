# core.show_role_catalog

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| name | text | NO |  | NO | NEVER |
| normalized_name | text | NO |  | NO | NEVER |
| is_active | boolean | NO | true | NO | NEVER |
| sort_order | integer | NO | 0 | NO | NEVER |
| metadata | jsonb | NO | '{}'::jsonb | NO | NEVER |
| created_by | text | YES |  | NO | NEVER |
| updated_by | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- show_id, normalized_name

## Foreign Keys

- show_id -> core.shows.id

## Indexes

- show_role_catalog_pkey (unique): id
- show_role_catalog_show_id_idx (non-unique): show_id, is_active, sort_order
- show_role_catalog_show_normalized_unique (unique): show_id, normalized_name

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "name": "example",
  "normalized_name": "example",
  "is_active": false,
  "sort_order": 0,
  "metadata": {},
  "created_by": "example",
  "updated_by": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```