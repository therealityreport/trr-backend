# Future Plan: Instagram ScraplingRuntime Implementation

Status: stub for a later implementation pass. The current posts and comments Scrapling lanes stay separate, supported worker lanes. The pluggable `ScraplingRuntime` must remain unhealthy/unsupported until this plan is implemented and verified.

## Required evidence before implementation

- Verify the current Scrapling package docs and APIs against the installed/pinned package in the backend venv. Current verified baseline: Scrapling 0.4.7, including compatible `StealthyFetcher.async_fetch` signature for existing lane call sites.
- Record response fixtures for profile warmup, timeline GraphQL, comments API success, auth failure, no-cookie warmup, transient transport failure, and non-JSON responses.
- Add contract tests for runtime health, unsupported endpoints, dispatcher gating, cookie/session adaptation, retry metadata, and rollback behavior.
- Define a dispatcher rollout strategy that proves `ScraplingRuntime` cannot intercept production traffic before it is healthy.
- Define operator rollback steps for returning traffic to the legacy scraper and the concrete posts/comments Scrapling lanes.

## Implementation outline

1. Replace the unsupported runtime scaffold with a version-checked adapter over the verified Scrapling APIs.
2. Keep posts and comments lane contracts stable while the runtime runs in shadow or manually invoked mode.
3. Promote dispatcher integration only after fixtures, contract tests, and operator rollback are in place.
4. Update the posts and comments runbooks with the runtime's real health states, supported endpoints, and smoke commands.
