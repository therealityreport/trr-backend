# core.cast_fandom

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| source | text | NO |  | NO | NEVER |
| source_url | text | NO |  | NO | NEVER |
| page_title | text | YES |  | NO | NEVER |
| page_revision_id | bigint | YES |  | NO | NEVER |
| scraped_at | timestamp with time zone | NO | now() | NO | NEVER |
| full_name | text | YES |  | NO | NEVER |
| birthdate | date | YES |  | NO | NEVER |
| birthdate_display | text | YES |  | NO | NEVER |
| gender | text | YES |  | NO | NEVER |
| resides_in | text | YES |  | NO | NEVER |
| hair_color | text | YES |  | NO | NEVER |
| eye_color | text | YES |  | NO | NEVER |
| height_display | text | YES |  | NO | NEVER |
| weight_display | text | YES |  | NO | NEVER |
| romances | ARRAY | YES |  | NO | NEVER |
| family | jsonb | YES |  | NO | NEVER |
| friends | jsonb | YES |  | NO | NEVER |
| enemies | jsonb | YES |  | NO | NEVER |
| installment | text | YES |  | NO | NEVER |
| installment_url | text | YES |  | NO | NEVER |
| main_seasons_display | text | YES |  | NO | NEVER |
| summary | text | YES |  | NO | NEVER |
| taglines | jsonb | YES |  | NO | NEVER |
| reunion_seating | jsonb | YES |  | NO | NEVER |
| trivia | jsonb | YES |  | NO | NEVER |
| infobox_raw | jsonb | YES |  | NO | NEVER |
| raw_html_sha256 | text | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

- person_id, source

## Foreign Keys

- person_id -> core.people.id

## Indexes

- cast_fandom_person_source_key (unique): person_id, source
- cast_fandom_pkey (unique): id
- core_cast_fandom_person_id_idx (non-unique): person_id
- core_cast_fandom_source_idx (non-unique): source

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "source": "example",
  "source_url": "example",
  "page_title": "example",
  "page_revision_id": 0,
  "scraped_at": "1970-01-01T00:00:00Z",
  "full_name": "example",
  "birthdate": "1970-01-01",
  "birthdate_display": "example",
  "gender": "example",
  "resides_in": "example",
  "hair_color": "example",
  "eye_color": "example",
  "height_display": "example",
  "weight_display": "example",
  "romances": [],
  "family": {},
  "friends": {},
  "enemies": {},
  "installment": "example",
  "installment_url": "example",
  "main_seasons_display": "example",
  "summary": "example",
  "taglines": {},
  "reunion_seating": {},
  "trivia": {},
  "infobox_raw": {},
  "raw_html_sha256": "example"
}
```