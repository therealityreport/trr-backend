# core.show_alternative_names

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | bigint | NO | nextval('core.show_alternative_names_id_seq'::regclass) | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| name | text | NO |  | NO | NEVER |
| language | text | YES |  | NO | NEVER |
| country | text | YES |  | NO | NEVER |
| source | text | NO | 'tmdb'::text | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- show_id -> core.shows.id

## Indexes

- show_alternative_names_pkey (unique): id
- show_alternative_names_show_id_idx (non-unique): show_id
- show_alternative_names_unique (unique): show_id, name, language, country, source

## RLS Enabled

true

## Example Row

```json
{
  "id": 0,
  "show_id": "00000000-0000-0000-0000-000000000000",
  "name": "example",
  "language": "example",
  "country": "example",
  "source": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```