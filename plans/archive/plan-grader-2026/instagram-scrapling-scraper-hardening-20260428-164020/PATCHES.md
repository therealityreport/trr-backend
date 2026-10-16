# Patches

This file maps required source-plan changes into the revised plan.

## Patch 1: Fix Shared Session Test

Original source plan created an `InstagramAuthSession` with only three fields:

```python
auth_session = InstagramAuthSession(
    cookies={"sessionid": "abc"},
    browser_account_id="bravotv",
    metadata={"source": "test"},
)
```

Revised plan uses `SimpleNamespace` because the adapter only needs `cookies`, `browser_account_id`, and `metadata` for this test:

```python
auth_session = SimpleNamespace(
    cookies={"sessionid": "abc"},
    browser_account_id="bravotv",
    metadata={"source": "test"},
)
```

## Patch 2: Add Posts Warmup Runtime Metadata Preservation

Original source plan added `InstagramPostsWarmupError` but did not update `posts_scrapling/job_runner.py` to preserve fetcher metadata when warmup fails.

Revised plan adds:

```python
except InstagramPostsWarmupError as exc:
    raise PostsScraplingRuntimeError(
        str(exc),
        error_code=exc.error_code,
        retryable=exc.retryable,
        runtime_metadata=dict(fetcher.runtime_metadata),
    ) from exc
```

## Patch 3: Make Cancellation Check Connection-Aware

Original source plan used `pg.fetch_one()` inside the comments target loop without a connection, which can add another pool checkout while `persist_conn` is already checked out.

Revised plan changes the helper signature:

```python
def _raise_if_cancelled(
    job_id: str,
    run_id: str | None,
    *,
    runtime_metadata: dict[str, Any] | None = None,
    conn: Any | None = None,
) -> None:
```

and calls it with `conn=persist_conn` inside the comments loop.

## Patch 4: Add Pre-Persist Cancellation Check

Original source plan only checked cancellation inside the comments loop. The revised plan checks once after warmup and before opening the persist connection, which makes cancellation cheap when the job has already been marked cancelled.

## Patch 5: Fix Markdown Fence Breakage

Original source plan nested a `bash` fence inside a `markdown` fence using the same backtick length. Revised runbook task uses four-backtick outer fences.

## Patch 6: Add Observable Success Signals

Original source plan mostly defined success as tests passing. Revised plan adds post-implementation observable outcomes:

- `ScraplingRuntime.healthcheck().reason == "scrapling_runtime_not_wired"` until implemented.
- transport failures return stable `transport_timeout` or `transport_error`.
- cancellation finishes jobs as `cancelled` between work units without extra comments-lane DB checkout.
- posts jobs record final `fetcher_runtime.request_count`.

