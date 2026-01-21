# core.person_images

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| source | text | NO |  | NO | NEVER |
| url | text | NO |  | NO | NEVER |
| width | integer | YES |  | NO | NEVER |
| height | integer | YES |  | NO | NEVER |
| caption | text | YES |  | NO | NEVER |
| is_primary | boolean | NO | true | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- person_id, source, url

## Foreign Keys

- person_id -> core.people.id

## Indexes

- person_images_person_id_idx (non-unique): person_id
- person_images_person_id_is_primary_idx (non-unique): person_id, is_primary) WHERE (is_primary = true
- person_images_person_source_url_unique (unique): person_id, source, url
- person_images_pkey (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "source": "example",
  "url": "example",
  "width": 0,
  "height": 0,
  "caption": "example",
  "is_primary": false,
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```