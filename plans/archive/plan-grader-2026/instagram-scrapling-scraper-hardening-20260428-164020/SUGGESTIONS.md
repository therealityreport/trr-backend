# Suggestions

These are optional follow-ups. They are not required for the revised plan to be execution-ready.

1. Title: Add a one-page Scrapling lane architecture diagram
   Type: Small
   Why: The posts/comments lanes, pluggable runtime scaffold, and legacy scraper can be confused.
   Where it would apply: `docs/workspace/instagram-posts-scrapling.md` and `docs/workspace/instagram-comments-scrapling.md`
   How it could improve the plan: Makes operator and reviewer handoff faster.

2. Title: Add a static no-cookie-values metadata scanner
   Type: Medium
   Why: Cookie-value leaks have already been a concern in Scraping metadata.
   Where it would apply: `tests/socials/` or a small script under `scripts/`
   How it could improve the plan: Prevents future metadata regressions beyond the two focused tests.

3. Title: Add a local fake Instagram response fixture pack
   Type: Medium
   Why: Current tests rely heavily on MagicMock response objects.
   Where it would apply: `tests/fixtures/instagram/scrapling/`
   How it could improve the plan: Makes response drift easier to review and update.

4. Title: Track retry reason counts in job metadata
   Type: Medium
   Why: A final `transport_error` alone hides whether retries were all proxy connect failures or mixed statuses.
   Where it would apply: posts/comments fetcher `runtime_metadata`
   How it could improve the plan: Better operator diagnosis without reading logs.

5. Title: Add worker restart note to local runbook
   Type: Small
   Why: Old worker processes can continue serving stale code after plan implementation.
   Where it would apply: `docs/workspace/instagram-comments-scrapling.md`
   How it could improve the plan: Reduces confusion during local validation.

6. Title: Split shared retry helpers into unit tests
   Type: Small
   Why: The helper behavior is simple and easy to lock.
   Where it would apply: `tests/socials/test_scrapling_http_utils.py`
   How it could improve the plan: Makes future retry changes safer.

7. Title: Add a final smoke command for one page only
   Type: Medium
   Why: Focused unit tests do not prove Scrapling/Patchright browser dependencies are present.
   Where it would apply: runbooks and optional final verification
   How it could improve the plan: Gives operators a bounded manual check without forcing live scrape in CI.

8. Title: Add cancellation latency to worker logs
   Type: Medium
   Why: Cooperative cancellation stops between units, not mid-request.
   Where it would apply: posts/comments job runners
   How it could improve the plan: Helps operators distinguish expected in-flight delay from a stuck worker.

9. Title: Add a short glossary for "runtime" vs "lane"
   Type: Small
   Why: `ScraplingRuntime` and `posts_scrapling`/`comments_scrapling` are distinct concepts.
   Where it would apply: runbooks and inline docs
   How it could improve the plan: Prevents implementation and support confusion.

10. Title: Create a future plan for actually implementing `ScraplingRuntime`
    Type: Large
    Why: This hardening plan intentionally marks it unsupported instead of wiring it.
    Where it would apply: `trr_backend/socials/instagram/runtimes/scrapling_runtime.py`
    How it could improve the plan: Separates operational hardening from a larger runtime implementation project.

