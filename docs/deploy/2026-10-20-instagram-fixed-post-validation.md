# Instagram fixed-post repair and validation — 2026-10-20

## Acceptance status

Implementation passed offline verification and review. All three detail targets
passed fresh durable retrieval. Full comment and reply retrieval has not passed.
Full-history launches, resumptions, retries, and dispatch for `bravotv`
remain gated; v2 ownership remains enabled. No full-history run was started.

The current source cooldown ends at 2026-10-20 20:30:39.741308 UTC
(4:30:39 p.m. Eastern). Automatic source attempts have stopped after two
consecutive resumed tranches reported `rate_limited` without committed progress.
Cooldown expiry alone does not reopen execution or prove access.

| Fixed post | Details retrieval | Parent retrieval | Reply retrieval |
| --- | --- | --- | --- |
| Photo `Dd37XeDly4U` | Passed: fresh source receipt | First page retained; continuation not attempted | Pending child-root query rejected in two resumed tranches |
| Carousel `Dd60pnHl_ux` | Passed: fresh source receipt | Held by shared cooldown | Not reached |
| Reel `Dd6m4XKDSQ1` | Passed: fresh source receipt | Held by shared cooldown | Not reached |

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
tag `ig-public-enabled-e27568bc`. This was the pre-release baseline.

The idle Mac LaunchAgent `com.trr.instagram-local-worker` was stopped and its
process exit verified before edits. Its prior configuration points at the editable
checkout; keep it stopped until the reviewed archive and environment are installed.
Retain the previous plist, environment and runtime for rollback. A provider
rollback does not restore secret values or configuration changes.

Release requires a committed Git archive, the same explicit Modal environment for
deployment and readiness, and a Mac launch configuration pointing to a reviewed
release directory. Core readiness probes were inspected: they do not perform
Instagram retrieval. Separate source probes must debit the shared request budget.

## Verified Modal release

Backend candidate commit: `98c6604bae5da16bad5091f04836af3c2eac26ce`.
Workspace contract commit: `a51b77a`. The archive's 2,537 tracked files were
byte-verified against the backend commit. Archive SHA-256:
`37f8838ef7beac7bfae5c235c1dde94e6121b86268a33727e46fa16a44244aac`.

The candidate deployed from that archive to the same verified target as **v84**,
tag `ig-fixed-post-98c6604b`, at 2026-10-20 11:45:41 Eastern. The deployment
helper's API cold-start canary returned HTTP 200 on its first attempt.

The required `--env main --probe-core-workers --strict-probes` readiness check
passed before and after deployment. All six core probes were healthy, with no
missing functions/endpoints or blocking failures. These probes issued no
Instagram requests. No secrets or provider configuration values were changed.
The preceding v83 remains in provider history and had fresh passing core readiness
evidence immediately before this deployment.

The deployed browser probe reports Playwright 1.64.0, Patchright 1.64.1,
curl-cffi 0.16.3, Scrapling 0.5 and Chromium 156.0.8078.4. Modal's existing
Python base remains 3.11.12; the explicit Homebrew Python update and rebuilt Mac
environment use 3.11.17. These are separate runtime identities.

Deployment and its health checks were completed during the existing Mac source
cooldown. This does not accept any retrieval cell or the full capability matrix.

## Preflight dispatch reconciliation

The final exclusivity check found one accepted dispatch from cancelled Modal run
`4e3c0cb5-efc3-4038-9708-fd5341db69b5`. Its target had no owner. Modal's call graph
confirmed `fc-01M5CSBZFBV5VXZAWHGM30MRZJ` was `SUCCESS` with no child calls.

At 15:53:13 UTC, a conditional transaction changed only dispatch owner token
`93adbd80-e770-4550-b09d-7fd7d0e614aa` from `accepted` to `expired`. The transaction
required that exact remote identity and generation, a cancelled newer run
generation, and no target owner. The before/after records were retained.
The remote identity, cancelled run, source evidence and checkpoints were preserved.
No source request or run resume occurred during reconciliation.

## First Mac tranche

Run `81127e19-8b72-4e32-9dd2-b12fc53c8101` used the committed release, exact
manifest hash `03a521624c38ab2d0e25e0a8b3b65938e288027551501af0357c8b36d16e03f9`,
and intent hash `c756fdf680d1b6ed512c3524041149c0bd594cde2e1dec118c0645c90ec06e17`.
It was configured to interrupt after a committed parent page.

At 15:57:03 UTC the photo's public document returned HTTP 200, 590,267 bytes,
with no redirect. Neither maintained parser yielded an acceptable media object.
Response SHA-256:
`f0b742dbfebf7fb6a848ef7419af04dfa0908a4cf2db662e8f2ceae3e0dd7878`.
Safe response metadata was retained privately; raw bootstrap tokens were not logged.

The durable result was `public_detail_unresolved`. The shared request gate
deferred further sends until 16:12:10 UTC. The other post details and all comments
were therefore unattempted in that run; they were not independently proven unavailable.
The CLI returned exit code 2 and `retrieval_not_passed`, then left the run
cancelled at generation 2 with zero target owners. The shared task ledger counted
one actual source request, leaving 199 in its rolling 24-hour allowance.

There were zero successful source evidence rows and zero committed comment pages.
No page existed to exercise the planned interruption/resume proof. Failure
handling passed for this incomplete document: counted request, retained diagnostic
metadata, explicit incomplete result, respected cooldown, and safe cancellation.
Retrieval and recovery acceptance did not pass.

The previous Mac source was separately archived from `348c9332`; SHA-256:
`8ef35a618ddbf10ccd64025a55fb1150524c4564f67e768d67b2531480b51a44`.
A private clone of its environment is pinned to the retained Python 3.11.15
binary and standard library. Imports and dependency consistency passed; it retains
psycopg2-binary 2.9.12, Playwright 1.62.0 and curl-cffi 0.16.1. This supplements the
original plist, original configuration, source patch and file-hash inventory.
Use the explicit cloned Python executable for recovery; its old console-script
shebangs still reference the original checkout.
## Transport diagnosis and second release

A bounded anonymous browser diagnosis retrieved the selected photo. Its observed
navigation headers alone did not repair the Requests response. The same headers
with curl-cffi's `chrome150` transport retrieved the native photo through the
production detail parser. This diagnostic result was not used as durable proof.
No actual comment-pagination operation was observed in the browser capture;
the load-more click issued `PolarisLoggedOutDynamicDialogQuery`. This alone does
not establish that a login is required, and no query names were guessed or changed.

Commit `e6fab2ce149423a96a59ba4256ea340c8040d5a6` selects that transport only for
public detail documents. The existing credential/cookie guard, request gate,
deadline, zero retries, and disabled redirects remain. Curl exceptions enter the
redacted failure path. Explicit `CurlOpt.PROXY=""` disables libcurl environment
proxy inheritance. Listing and comment transports remain unchanged.

The existing detail/refresh checks passed (57 tests), and eight new adapter checks
passed, including a real curl option boundary intercepted before network I/O.
Independent review found the environment-proxy gap, which was fixed and verified;
no other findings remained. Ruff and scoped diff checks passed.

One reviewer probe may have reached a public permalink once because its attempted
interception was ineffective. No response metadata was retained. One request was
conservatively charged to the shared ledger; it is not acceptance evidence. The
reviewer stopped further probes. No response-specific cooldown can be inferred
from that call. All subsequent source work used the production gate and budget.

The new Git archive contains 2,539 byte-verified tracked files, SHA-256
`a6487cc10c5d6d1de5155d4c983b985311926bb10920eb9371eaac0625bfea7d`.
Its isolated Mac environment was synchronized from the unchanged reviewed lock;
all 155 installed packages passed the dependency check. Matching browser binaries
were copied into the release. The previous release remains intact for rollback.

The same archive deployed to `admin-56995/main/trr-backend-jobs` as **v85**, tag
`ig-fixed-curl-e6fab2ce`, at 2026-10-20 12:28:33 Eastern. The API cold-start canary
returned HTTP 200 on attempt one. The required strict core readiness checks passed
with all six core probes healthy and no missing or blocking entries. This proves
runtime health, not public retrieval from Modal's network. v84 and v83 remain in
provider history. No secret or provider configuration was changed.

## Durable detail success and parent-page interruption

New run `dbc80df9-0df7-43ca-9abc-2f1d1e393056` freezes source `e6fab2ce`, manifest
hash `f9838e3e7b4ab52f5ec9964baa7bd060e26f1a155402cb6e19c71d21c6df0def`,
and the unchanged exact September post selection and intent hash. The earlier
run and its evidence were preserved.

All three detail targets committed fresh evidence and matching source receipts,
one request each. The photo then committed its first public parent page: 11 native
comment identities, a non-exhausted `after` cursor, and persisted provenance
`PolarisPostCommentsPaginationQuery` / document `26297736713236852`. This is live
proof of that maintained query's first page, not proof of every pagination query.
The page receipt is `8c0cf24a-4dd2-4632-b458-754a3953de5b`; its content hash is
`8da696d4c7c06c7e8d78dea7eb64a72f3168fbcadfbf3e9c01267ea9c0a81a61`.
The harness intentionally cancelled after the committed parent page (generation 2).

The same run resumed at generation 3. Its next comment attempt made two counted
requests and received a `rate_limited` outcome. The shared public lane deferred all
further source calls until **2026-10-20 18:30:10.295022 UTC (2:30:10 p.m. Eastern)**.
The harness preserved the first page and cancelled at generation 4. Resume cursor
correctness, later pages, native parent/child exhaustion, qualifying multi-page
replies, and a fresh anonymous repeat remain unverified. Returned parent counts
were null, not proved zero; unknown reply coverage must remain unresolved.

The ledger now contains 63 charged requests, including the conservative reviewer
charge, with 137 remaining in the rolling 24-hour allowance. Budget headroom does
not override the provider cooldown. No Instagram request is permitted by this
validation workflow before the recorded cooldown expires.

## Mac configuration and remaining gate

The installed LaunchAgent now points at the isolated `e6fab2ce` release and its
private environment, not the editable checkout. Its installed plist SHA-256 is
`80fb36aea9f6cd5d55ed24933f9a6716a845666afab9ed7a1a1e1cfa5c8d25b7`.
The prior plist, environment, source archive and dependency environment are retained.
The service remains stopped during incomplete validation. No full-history run
has started, and the admission gate remains closed.

After the provider cooldown, resume the same frozen run with its private state and
shared account budget. Do not restart from root or overwrite the manifest. Complete
parent/child native exhaustion and the independent fresh-session repeat before
accepting the nine-cell matrix or opening full history. The first failed run and
the first committed parent page are retained as recovery evidence.

## Bounded resume and stop-rule close-out — 18:31 UTC

This section supersedes the preceding instruction to resume after cooldown.
Validation remains incomplete; this is an operational stop, not capability
acceptance or proof of permanent logged-out unavailability.

The implementation session verified the original private manifest, state and
shared ledger, all 2,539 installed release files against the frozen archive/source,
and the installed LaunchAgent identity. The workspace relocation resolves through
the existing `/Users/thomashulihan/Projects` symlink; the isolated release and
private configuration do not depend on the moved checkout. The worker was
unloaded and no matching worker or harness process was present before execution.

A read-only database snapshot verified project `vwxfvzutyufrkhfgoeaa`, the frozen
run at generation 4, unchanged manifest/intent, local placement, no pending control
command, no competing run/owner/uncertain dispatch, and the source cooldown ending
at 18:30:10.295022 UTC. The original parent-page receipt, evidence hash and eleven
canonical comment mappings were intact. No full-history run existed.

The parent resumed run `dbc80df9-0df7-43ca-9abc-2f1d1e393056` using source
`e6fab2ce149423a96a59ba4256ea340c8040d5a6`, the original private files and shared
ledger, and `--interrupt-after child`. The clean process environment loaded the
same private release configuration. No manifest, source, query identity,
transport selection or request limit was changed.

| Resumed generation | First charged request (UTC) | Charged requests | Result | New committed pages |
| --- | --- | --- | --- | --- |
| 3, then cancelled at 4 | 16:30:07.610590 | 2 | `rate_limited`; remaining targets held | 0 |
| 5, then cancelled at 6 | 18:30:37.296826 | 2 | `rate_limited`; remaining targets held | 0 |

The new tranche ended with exit code 2, `retrieval_not_passed`, and
`cooldown_or_wait`. The production source gate now records two consecutive
failures and defers requests until 20:30:39.741308 UTC. The shared rolling ledger
contains 65 charges, with 135 remaining and no open tranche. No retry was made
inside either rejection's cooldown.

### What the rejection evidence establishes

Both failed resumes reached the same pending child-root query, not the parent's
saved `after` cursor. The frozen fetcher hydrates pending children before
continuing parent pagination. Retained evidence identifies
`PolarisPostChildCommentsQuery` / document `34884685271179117`, root provenance,
page size 12, popular sort, a matching parent identity, and null request cursor:

- Earlier rejection evidence: `4663ad8b-a300-4ac1-b0d2-51223292700b`,
  captured at 16:30:09.957407 UTC.
- New rejection evidence: `b74c7c7d-6942-44ef-be1c-7eaee6423cc1`,
  captured at 18:30:39.425862 UTC.
- Both retained payloads have SHA-256
  `bd80e05e4f86be9af646f14efdf0f4b41fbd330f82a40011ec244469bcf4500e`.

The runtime reported `rate_limited`, but exact HTTP status and the original
rate-limit marker are not independently recoverable from these sanitized
payloads. Reclassifying the retained payload yields `source_incomplete`, and
the safe failure-basis projection contains no retained rate marker. Do not
describe this as proven HTTP 429, permanent anonymous unavailability, or a
successfully transmitted parent continuation.

A private Requests metadata observer was checked offline, including logging
failures, before this tranche. It made no extra requests and did not alter request
arguments. It captured no comment calls because durable public comments use
`httpx.AsyncClient`, with `trust_env=False` and `follow_redirects=False`. It
therefore supplies no wire-level cursor/authentication proof. No replay was made
to recover telemetry. Anonymous configuration evidence remains the verified
frozen public adapter with empty credential inputs, no proxy and the production
credential/request guards; it is distinct from independent wire evidence.

### Preserved state and final acceptance matrix

The post-tranche read-only snapshot confirmed generation 6, cancelled state,
no target owners, no open budget tranche, and all six account runs cancelled.
The original page receipt `8c0cf24a-4dd2-4632-b458-754a3953de5b`, its evidence and
content hash are unchanged. Before/after observation hashes match. All eleven
native IDs still map to eleven distinct canonical rows with zero identity
mismatches. No page or identity was lost. This proves preservation through this
failed resume, not successful continuation or child-page interruption.

| Fixed post | Details | Parents | Replies |
| --- | --- | --- | --- |
| Photo `Dd37XeDly4U` | PASS: retained fresh receipt | INCOMPLETE: one page, eleven IDs, saved cursor; continuation untested | BLOCKED: same child-root query failed twice; no committed child page |
| Carousel `Dd60pnHl_ux` | PASS: retained fresh receipt | NOT REACHED: shared cooldown | NOT REACHED |
| Reel `Dd6m4XKDSQ1` | PASS: retained fresh receipt | NOT REACHED: shared cooldown | NOT REACHED |

The two consecutive eligible resumes continued the frozen run's pending child
work and produced no committed progress. They satisfy the accepted stop rule;
the rule does not establish a pagination-cursor continuation. Automatic source
attempts stop here. The multi-page parent/child requirements, deliberate committed
child interruption/resume, and independent anonymous repeat remain unproven.
No fresh repeat was started and no lane was restarted from root.

The harness still reports `capability_matrix_acceptance: not_evaluated`.
An additional source-level constraint remains relevant to later work: current
comment settlement requires a known expected overall count. Unknown reply
counts can be resolved by native lane/leaf proof, but native exhaustion alone
does not satisfy an unknown overall count. This was inspected, not changed or
established as the cause of the live rejection.

The full-history gate remains installed and the general Mac worker remains
unloaded. The approved stop requires a next-step decision before more source
attempts; expiration of the new cooldown is not that decision. Any later repair
must preserve this frozen run and use a new source-labelled validation run.

Two native investigators supplied artifact/runtime checks and evidence mapping.
Their routing preparations succeeded and both returned results. Router lifecycle
reconciliation returned `HOST_ERROR: thread not loaded` for both child IDs, so
router acceptance and exact delivered-instruction matching remain unverified.
The parent retained their reports and checked the live database/run outcomes.

Only this validation record changed in the repository. No production code, SQL
schema, dependency, secret, deployment or service configuration changed. Existing
passing offline checks were reused; the new private observer received an offline
stub check. Modal remains at the previously recorded v85 identity; no new Modal
verification or deployment was needed or performed. TRR-APP build is not
applicable to this validation/documentation close-out.
