# core.sources

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | text | NO |  | NO | NEVER |
| category | text | NO |  | NO | NEVER |
| aliases | ARRAY | YES | '{}'::text[] | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- sources_pkey (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "id": "example",
  "category": "example",
  "aliases": [],
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```