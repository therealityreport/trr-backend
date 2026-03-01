# core.season_fandom

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| season_id | uuid | NO |  | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| season_number | integer | NO |  | NO | NEVER |
| source | text | NO |  | NO | NEVER |
| source_url | text | NO |  | NO | NEVER |
| page_title | text | YES |  | NO | NEVER |
| page_revision_id | bigint | YES |  | NO | NEVER |
| scraped_at | timestamp with time zone | NO | now() | NO | NEVER |
| summary | text | YES |  | NO | NEVER |
| dynamic_sections | jsonb | YES |  | NO | NEVER |
| citations | jsonb | YES |  | NO | NEVER |
| conflicts | jsonb | YES |  | NO | NEVER |
| source_variants | jsonb | YES |  | NO | NEVER |
| ai_model | text | YES |  | NO | NEVER |
| ai_generated_at | timestamp with time zone | YES |  | NO | NEVER |
| raw_html_sha256 | text | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- season_id -> core.seasons.id
- show_id -> core.shows.id

## Indexes

- core_season_fandom_season_number_idx (non-unique): season_number
- core_season_fandom_show_id_idx (non-unique): show_id
- season_fandom_pkey (unique): id
- season_fandom_season_source_key (unique): season_id, source

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "season_id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "season_number": 0,
  "source": "example",
  "source_url": "example",
  "page_title": "example",
  "page_revision_id": 0,
  "scraped_at": "1970-01-01T00:00:00Z",
  "summary": "example",
  "dynamic_sections": {},
  "citations": {},
  "conflicts": {},
  "source_variants": {},
  "ai_model": "example",
  "ai_generated_at": "1970-01-01T00:00:00Z",
  "raw_html_sha256": "example"
}
```