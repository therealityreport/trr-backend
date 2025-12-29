# core.episode_cast

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| episode_id | uuid | NO |  | NO | NEVER |
| cast_membership_id | uuid | NO |  | NO | NEVER |
| appearance_type | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- episode_id, cast_membership_id

## Foreign Keys

- cast_membership_id -> core.cast_memberships.id
- episode_id -> core.episodes.id

## Indexes

- episode_cast_cast_membership_id_idx (non-unique): cast_membership_id
- episode_cast_episode_id_idx (non-unique): episode_id
- episode_cast_episode_id_membership_unique (unique): episode_id, cast_membership_id
- episode_cast_pkey (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "episode_id": "00000000-0000-0000-0000-000000000000",
  "cast_membership_id": "00000000-0000-0000-0000-000000000000",
  "appearance_type": "example",
  "created_at": "1970-01-01T00:00:00Z"
}
```