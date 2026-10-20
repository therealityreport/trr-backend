# Instagram fixed-post repair and validation — 2026-10-20

## Acceptance status

Candidate implementation passed offline verification and review. Live retrieval has not
passed. Full-history launches, resumptions, retries, and dispatch for `bravotv`
remain gated; v2 ownership remains enabled. No full-history run was started.

The previous Mac run's recorded source cooldown ends at
2026-10-20 15:56:20.832068 UTC. No task Instagram requests were sent before that
deadline. A cooldown expiry permits a bounded attempt; it does not prove access.

| Fixed post | Details retrieval | Parent retrieval | Reply retrieval |
| --- | --- | --- | --- |
| Photo `Dd37XeDly4U` | Not run | Not run | Not run |
| Carousel `Dd60pnHl_ux` | Not run | Not run | Not run |
| Reel `Dd6m4XKDSQ1` | Not run | Not run | Not run |

Catalog identities select September 2026 posts outside the latest twelve. Catalog
counts and metadata are setup provenance, never fresh retrieval evidence.
Unavailable, cached, interrupted, or unresolved outcomes do not pass validation.
Parent acceptance requires multiple pages and native exhaustion on each post.
Reply acceptance requires actual parent-linked replies and a multi-page child
lane on at least one qualified thread. Live interruption/resume and a fresh
anonymous repeat remain required.

## Candidate behavior

- The public detail adapter tries the maintained public HTML extractor on the
  already fetched response when the permalink parser finds no media. It does
  not issue another request or manufacture unknown fields.
- Repeated public cursors become `cursor_stalled` after committing the page.
  Ordinary retries retain the block. An explicit cancelled-run lane restart
  records the old lane, its hash, operator reason, source revision and command
  identity, then starts only that lane at its root with canonical deduplication.
- `scripts/ingestion/validate_fixed_posts.py` freezes exact catalog identities,
  uses the production scheduler, worker, public adapters and persistence, and
  forces fresh detail work. It supports inspection, cancellation, resume,
  committed-page interruption and audited lane restart.
- The operator's shared private budget limits a single active stream to 50
  document/API requests or 15 minutes per tranche and 200 requests per rolling
  24 hours. All task probes must use that same ledger. Provider cooldowns can
  impose longer waits. Competing runs or unresolved owners block execution.
- Optional Scrapling production runtime remains disabled. Browser capture is
  reserved for bounded diagnosis. No login, proxy or paid provider is introduced.

## Dependency dispositions

| Component | Candidate / local version | Disposition |
| --- | --- | --- |
| Homebrew PostgreSQL tools | 17.11_1 | Updated within major 17; temporary test clusters only |
| Homebrew libpq | 18.6_1 | Updated client tools; no hosted database update |
| Homebrew Python | 3.11.17 | Updated; new private validation environment |
| Standalone uv | 0.13.3 | Updated using self-update; previous binaries retained |
| Playwright / Patchright | 1.64.0 / 1.64.1 | Updated affected locks and private matching Chromium |
| curl-cffi | 0.16.3 | Updated affected backend and Modal locks |
| psycopg2-binary | 2.9.13 | Updated affected backend and Modal locks; bundled libpq 17.11 |
| psycopg / binary | 3.3.6 | Retained; bundled libpq 18.6 verified |
| Scrapling / Requests / HTTPX | 0.5 / 2.34.2 / 0.28.1 | Retained current compatible versions |
| DBOS / pytest | 3.3.0 / 9.1.1 | Retained |
| OpenSSL 3 / ICU | 3.6.3 / 78.3 | Preserved; offered OpenSSL downgrade excluded |
| Other PostgreSQL/Python majors | Existing versions | Deferred: no active dependency requires their update |
| Operator-cookie environment | Existing lock | Deferred: separate authenticated workflow outside public-only scope |

Homebrew dependencies also updated ca-certificates, krb5, readline and xz, and
installed OpenSSL 4 for the updated formula dependencies. Global OpenSSL 3 links
were restored; Python reports OpenSSL 3.6.3. Python installation used Homebrew's
`--ignore-dependencies` after verifying every dependency to avoid the offered
OpenSSL downgrade. Prior kegs and the original backend environment remain.
No existing database cluster was migrated and no unrelated service was restarted.

## Verification and review

The fixture repair permits only explicitly registered temporary PostgreSQL
host/port/database/user combinations. Ambient credentials, service overrides,
unregistered databases and unrelated networking remain blocked.

- Public transport/comment and validation gate tests: 141 passed.
- Audited parent/child lane restart regressions: 2 passed.
- Related composition, command, worker and local execution checks: 166 passed;
  the exact restricted-role lease fixture follow-up passed separately (1 test).
- Admission tests after bounded-fixture correction, including full-history
  denial before a capacity probe: 41 passed.
- App capability/proxy consumers: 31 passed across two files. No production app
  build was requested or run.
- New environment dependency check and Python standard-library imports passed.
  Isolated PostgreSQL tests pass with the updated tools. Scrapling local network
  fixture and standalone Playwright Chromium launch passed.
- Harness tests in the updated environment: 21 passed. The unavailable-outcome
  regression passed again after a result-label clarification.
- Independent review found one harness false-success defect: generic workflow
  completion could include unavailable targets. The repair separates workflow
  state from retrieval validation, checks fresh retained evidence and committed
  receipts, and returns exit code 2 for a retrieval nonpass. Full capability
  acceptance remains separate. Fix-only review passed. No other qualifying
  defect was found in the reviewed batch.

## Source, deployment and rollback

Starting backend revision: `348c9332267074572756c0c66bf4d3e16a7d1076`,
branch `chore/repo-hygiene-2026-09-25`. Unrelated dirty source was inventoried,
hashed and preserved privately. Only task-owned paths may enter the release.

Live provider baseline was verified as workspace `admin-56995`, environment
`main`, app `trr-backend-jobs` (`ap-DkLTRoSvqhbkGO7fHyHxrD`), deployment `v83`,
tag `ig-public-enabled-e27568bc`. No candidate deployment has occurred yet.

The idle Mac LaunchAgent `com.trr.instagram-local-worker` was stopped and its
process exit verified before edits. Its prior configuration points at the editable
checkout; keep it stopped until the reviewed archive and environment are installed.
Retain the previous plist, environment and runtime for rollback. A provider
rollback does not restore secret values or configuration changes.

Release requires a committed Git archive, the same explicit Modal environment for
deployment and readiness, and a Mac launch configuration pointing to a reviewed
release directory. Core readiness probes were inspected: they do not perform
Instagram retrieval. Separate source probes must debit the shared request budget.
