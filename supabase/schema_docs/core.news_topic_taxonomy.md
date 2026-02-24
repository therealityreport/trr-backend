# core.news_topic_taxonomy

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| topic_key | text | NO |  | NO | NEVER |
| keywords | ARRAY | NO | '{}'::text[] | NO | NEVER |
| enabled | boolean | NO | true | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

topic_key

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- news_topic_taxonomy_pkey (unique): topic_key

## RLS Enabled

false

## Example Row

```json
{
  "topic_key": "example",
  "keywords": [],
  "enabled": false,
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```