# Instagram public ingestion activation — 2026-10-20

## Activated runtime

- Provider: Modal workspace `admin-56995`, environment `main`, app
  `trr-backend-jobs` (`ap-DkLTRoSvqhbkGO7fHyHxrD`).
- Deployment: `v83`, tag `ig-public-enabled-e27568bc`.
- Source: `e27568bcd1afae0ddd96aac2424c3fb3b2245051`, deployed from a Git archive.
  Existing unrelated uncommitted changes were excluded and preserved.
- Runtime secret: configured the v2 enabled/coordinator/schema flags, verified
  DBOS URL and runtime role, account policy, explicit workspace, public scrape
  mode, and no comments proxy provider. No secret values belong in this record.
- Supabase target: `vwxfvzutyufrkhfgoeaa`. Restricted runtime login and DBOS
  migration 123 passed readiness checks. No migration or password reset was needed.
- API health and strict core-worker readiness passed. The authenticated v2 API
  reports enabled actions and an online local worker. Authentication uses the TRR
  internal-admin contract; it does not authenticate to Instagram.
- Mac LaunchAgent: `com.trr.instagram-local-worker`, installed for the current
  user with RunAtLoad and KeepAlive. Its private, ignored `.env.local-worker`
  includes business-database and storage settings as well as DBOS settings.
  The launcher does not load `.env` automatically.
- An idle LaunchAgent restart changed the worker PID and restored the online
  capability. Both cancelled runs retained their request counts and zero target
  owners afterward. This checks service restart, not interrupted-run replay.

## Bounded acceptance

Both tests used `bravotv`, a six-hour window, comments and media, one worker per
stage, and disabled commenter-media follow-ups. An independent watchdog was armed
before launch. Three minutes or 20 recorded requests were cancellation triggers,
not hard ceilings: an in-flight request could finish before cancellation.

| Placement | Run | Result |
| --- | --- | --- |
| Modal | `4e3c0cb5-efc3-4038-9708-fd5341db69b5` | Public bootstrap redirected to login. No login was attempted. Cancelled at one recorded request, generation 2, no target owners. |
| Mac | `6c3717cf-167f-4d1c-b12d-8a6208545337` | Two listing pages committed and one post selected (`Det5B7akvkK`). Comments hit a rate limit. Cancelled at seven recorded requests (four listing, three comments), generation 2, no target owners. |

The Mac run retained two listing and two comment evidence records. The comment
target remained unresolved; retained evidence is not complete comment coverage.
Media had not started when cancellation occurred. Its stored comment retry time
was 2026-10-20 15:56:20 UTC; cancellation prevents automatic retry.

The first Modal launch timed out at the HTTP client, but its idempotency key
resolved to a saved run. It was not submitted again with a new key. The Mac launch
and both cancellation commands returned HTTP 202 through the TRR API.

Provider readiness probes also issued public Instagram smoke requests outside
these two run counters. The counters above are run-ledger counts, not a total of
all Instagram traffic during activation.

## Acceptance boundary and recovery

Full history was not started. Database access, API authorization, deployment,
worker dispatch, local listing persistence, and cancellation have live evidence.
Complete comments, media transfer, and interrupted in-flight replay remain
unverified. No browser acceptance or app build was performed for this operational
activation; application source was unchanged.

For the next small acceptance test, use the same six-hour Mac window with media
only after applicable public-source cooldowns expire. Validate comments separately
without interpreting partial or unavailable replies as complete coverage.

Previous provider deployment `v81` (`insightface-d1ef24a8-on-v80`) remains in Modal
history. Provider rollback alone does not restore secret values. To disable new
v2 work, set the v2 enabled/coordinator flags to zero and redeploy to this same
explicit environment. Stop the Mac service with its exact LaunchAgent label if
needed; stopping the worker does not cancel saved runs.
