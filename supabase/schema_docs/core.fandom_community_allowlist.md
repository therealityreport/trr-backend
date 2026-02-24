# core.fandom_community_allowlist

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| domain | text | NO |  | NO | NEVER |
| is_active | boolean | NO | true | NO | NEVER |
| updated_by | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

domain

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- core_fandom_community_allowlist_active_idx (non-unique): is_active) WHERE (is_active = true
- fandom_community_allowlist_pkey (unique): domain

## RLS Enabled

false

## Example Row

```json
{
  "domain": "example",
  "is_active": false,
  "updated_by": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```