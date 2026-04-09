# core.fandom_page_directory

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| community_domain | text | NO |  | NO | NEVER |
| page_title | text | NO |  | NO | NEVER |
| page_slug | text | NO |  | NO | NEVER |
| page_url | text | NO |  | NO | NEVER |
| source_kind | text | NO | 'allpages_html'::text | NO | NEVER |
| is_active | boolean | NO | true | NO | NEVER |
| first_seen_at | timestamp with time zone | NO | now() | NO | NEVER |
| last_seen_at | timestamp with time zone | NO | now() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- fandom_page_directory_domain_active_last_seen_idx (non-unique): community_domain, is_active, last_seen_at DESC
- fandom_page_directory_domain_slug_active_idx (non-unique): community_domain, page_slug) WHERE (is_active = true
- fandom_page_directory_domain_title_active_idx (non-unique): community_domain, page_title) WHERE (is_active = true
- fandom_page_directory_domain_url_idx (unique): community_domain, page_url
- fandom_page_directory_pkey (unique): id

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "community_domain": "example",
  "page_title": "example",
  "page_slug": "example",
  "page_url": "example",
  "source_kind": "example",
  "is_active": false,
  "first_seen_at": "1970-01-01T00:00:00Z",
  "last_seen_at": "1970-01-01T00:00:00Z",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```