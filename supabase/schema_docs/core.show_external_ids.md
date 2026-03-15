# core.show_external_ids

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | bigint | NO | nextval('core.show_external_ids_id_seq'::regclass) | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| source_id | text | NO |  | NO | NEVER |
| external_id | text | NO |  | NO | NEVER |
| is_primary | boolean | NO | true | NO | NEVER |
| valid_from | date | YES |  | NO | NEVER |
| valid_to | date | YES |  | NO | NEVER |
| observed_at | timestamp with time zone | NO | now() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- show_id -> core.shows.id
- source_id -> core.sources.id

## Indexes

- show_external_ids_pkey (unique): id
- show_external_ids_primary_uq (unique): show_id, source_id) WHERE (is_primary = true
- show_external_ids_show_id_idx (non-unique): show_id
- show_external_ids_unique_active_handles_uq (unique): source_id, external_id) WHERE ((source_id = ANY (ARRAY['twitter'::text, 'instagram'::text, 'facebook'::text, 'tiktok'::text, 'youtube'::text])) AND (valid_to IS NULL)
- show_external_ids_unique_identifiers_uq (unique): source_id, external_id) WHERE (source_id = ANY (ARRAY['imdb'::text, 'tmdb'::text, 'wikidata'::text, 'tvdb'::text, 'tvrage'::text])

## RLS Enabled

true

## Example Row

```json
{
  "id": 0,
  "show_id": "00000000-0000-0000-0000-000000000000",
  "source_id": "example",
  "external_id": "example",
  "is_primary": false,
  "valid_from": "1970-01-01",
  "valid_to": "1970-01-01",
  "observed_at": "1970-01-01T00:00:00Z",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```