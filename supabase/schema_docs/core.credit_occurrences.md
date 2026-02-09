# core.credit_occurrences

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| credit_id | uuid | NO |  | NO | NEVER |
| episode_id | uuid | NO |  | NO | NEVER |
| appearance_type | text | NO | 'appears'::text | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| air_year | integer | YES |  | NO | NEVER |
| credit_text | text | YES |  | NO | NEVER |
| attributes | jsonb | NO | '[]'::jsonb | NO | NEVER |
| is_archive_footage | boolean | NO | false | NO | NEVER |

## Primary Key

credit_id, episode_id

## Unique Constraints

(none)

## Foreign Keys

- credit_id -> core.credits.id
- episode_id -> core.episodes.id

## Indexes

- credit_occurrences_episode_credit_idx (non-unique): episode_id, credit_id
- credit_occurrences_episode_id_idx (non-unique): episode_id
- credit_occurrences_pkey (unique): credit_id, episode_id

## RLS Enabled

true

## Example Row

```json
{
  "credit_id": "00000000-0000-0000-0000-000000000000",
  "episode_id": "00000000-0000-0000-0000-000000000000",
  "appearance_type": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "air_year": 0,
  "credit_text": "example",
  "attributes": {},
  "is_archive_footage": false
}
```