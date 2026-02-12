# core.entity_links

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| entity_type | text | NO |  | NO | NEVER |
| entity_id | uuid | NO |  | NO | NEVER |
| show_id | uuid | YES |  | NO | NEVER |
| season_number | integer | NO | 0 | NO | NEVER |
| link_group | text | NO |  | NO | NEVER |
| link_kind | text | NO |  | NO | NEVER |
| label | text | YES |  | NO | NEVER |
| url | text | NO |  | NO | NEVER |
| url_key | text | NO |  | NO | NEVER |
| status | text | NO | 'pending'::text | NO | NEVER |
| confidence | numeric | YES |  | NO | NEVER |
| discovered_by | text | YES |  | NO | NEVER |
| source | text | YES |  | NO | NEVER |
| metadata | jsonb | NO | '{}'::jsonb | NO | NEVER |
| created_by | text | YES |  | NO | NEVER |
| updated_by | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- entity_type, entity_id, link_kind, season_number, url_key

## Foreign Keys

- show_id -> core.shows.id

## Indexes

- entity_links_entity_idx (non-unique): entity_type, entity_id
- entity_links_group_idx (non-unique): link_group
- entity_links_pkey (unique): id
- entity_links_show_id_idx (non-unique): show_id
- entity_links_status_idx (non-unique): status
- entity_links_unique_active (unique): entity_type, entity_id, link_kind, season_number, url_key

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "entity_type": "example",
  "entity_id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "season_number": 0,
  "link_group": "example",
  "link_kind": "example",
  "label": "example",
  "url": "example",
  "url_key": "example",
  "status": "example",
  "confidence": 0,
  "discovered_by": "example",
  "source": "example",
  "metadata": {},
  "created_by": "example",
  "updated_by": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```