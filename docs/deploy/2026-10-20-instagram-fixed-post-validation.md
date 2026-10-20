# Instagram fixed-post repair and validation — 2026-10-20

## Acceptance status

Implementation passed offline verification and review. All three detail targets
passed fresh durable retrieval. Full comment and reply retrieval has not passed.
Full-history launches, resumptions, retries, and dispatch for `bravotv`
remain gated; v2 ownership remains enabled. No full-history run was started.

The current source cooldown ends at 2026-10-20 22:32:52.871886 UTC
(6:32:52 p.m. Eastern). The user-authorized instrumented retry completed at
20:32 UTC and independently captured an HTTP 200 GraphQL rate rejection.
Three consecutive resumed tranches have now reported `rate_limited` without
committed progress. Source attempts have stopped and the one-shot follow-up is
paused. Cooldown expiry alone does not reopen execution or prove access.

| Fixed post | Details retrieval | Parent retrieval | Reply retrieval |
| --- | --- | --- | --- |
| Photo `Dd37XeDly4U` | Passed: fresh source receipt | First page retained; continuation not attempted | Pending child-root query rejected in three resumed tranches |
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

## HTTPX instrumentation repair and authorized follow-up — 19:38 UTC

The user explicitly authorized subagents to repair the instrumentation, diagnose
the child-query failure, and make one bounded retry after cooldown while keeping
full history gated. This is a specific exception to the preceding operational
stop; it does not authorize repeated resumed tranches or full-history execution.

The source investigator traced the evidence discrepancy. The public comment
adapter classifies the native response before retaining a sanitized payload.
Sanitization removes native messages, errors and rate flags; replay of that saved
projection can therefore report `source_incomplete` while the original runtime
reported `rate_limited`. Safe diagnostic ContextVar values must be read inside
the adapter's async callback, because writes inside `asyncio.run()` do not reach
the outer caller. No classifier repair is supported by the retained evidence.

The maintained child query and its recorded provenance agree. Actual child
variables contain the logged-in provider flag, `first`, `is_chronological`,
`media_id`, `parent_comment_id`, and optional `after`. There is no child
`sort_order` variable; the retained popular-sort label is provenance. A remote
query change remains possible but unproven. No query or transport was switched.

A private observer now wraps the actual `httpx.AsyncClient.send` boundary. It
records UTC/sequence, known query and document identity, header/form agreement,
bounded flags, hashed media/parent/cursor values, prepared credential-presence
booleans, client environment/redirect settings, exact HTTP status, buffered-body
size/hash, Retry-After, and the existing safe native failure projection. It
examines only buffered content inside the coroutine and handles the native
`for (;;);` JSON prefix. It adds no requests, preserves arguments, response and
exception identity, and isolates logging failures. Raw bodies, credentials,
cookies and arbitrary native messages are excluded.

The private one-shot runner preserves source `e6fab2ce`, the original manifest,
state, run and ledger. It checks the time floor and live public gate, cancelled
generation 6/local placement, absent competing runs/owners/worker, budget, and
the original page/mappings before invoking the unchanged CLI with
`resume --interrupt-after child --apply`. An exclusive persisted marker prevents
replaying a started or uncertain invocation. The production per-request guards,
single-stream lock, 50-request/15-minute limit and rolling daily limit still apply.

Ten offline observer tests passed. Four offline runner cases passed: ready,
blocked gate, changed generation and existing owner. An actual invocation before
the cooldown was rejected before source or control work. Independent review of
the complete observer, tests and runner found no blocking issue. Missing telemetry
must remain unresolved; it cannot be treated as proof of anonymous transmission
or native failure classification. Router reconciliation again could not load the
native child identity; routed acceptance/exact instruction matching remain
unverified, separate from the retained native reports and passing checks.

The app follow-up `instagram-instrumented-retry` is active for one occurrence at
**2026-10-20 16:32 America/New_York (20:32 UTC)** in this chat. The reviewed runner
also refuses execution before 20:30:40 UTC and checks the current database gate.
At this preparation checkpoint, no additional Instagram request or run resume has
occurred. The expected result is pending, not a retrieval pass.

Scrapling investigation found that the old browser harness uses an older release,
run and cooldown and cannot be reused unchanged. Prior retained metadata shows
one load-more click and `PolarisLoggedOutDynamicDialogQuery`, with no actual
comment pagination request. No retained HTML/HAR is available for useful offline
Selector analysis. If the instrumented retry still fails, inspect its native-safe
diagnostics first; any necessary fresh Scrapling capture must use a reviewed,
anonymous, tightly bounded harness with the same budget and current cooldown.
Browser evidence remains diagnostic and cannot satisfy durable retrieval cells.

These repairs affect private diagnostic files only. Production source, SQL,
secrets, deployment and service configuration are unchanged. No Modal deployment
or app build applies. Full history remains gated and the general worker stays
unloaded.

## Instrumented retry result — 20:33 UTC

The one authorized retry ran after the recorded cooldown and fresh guarded
preflight. It resumed the frozen run at generation 7, made two charged requests,
returned exit code 2 (`retrieval_not_passed`, `cooldown_or_wait`), and cancelled
at generation 8. No new comment page was committed. The one-shot app follow-up
is now paused; the durable started marker prevents replay.

| Request time (UTC) | Request | HTTP | Native result |
| --- | --- | --- | --- |
| 20:32:50.592066 | Anonymous document bootstrap | 200 | Buffered HTML, 856,158 bytes |
| 20:32:52.461056 | Pending child-root GraphQL query | 200 | 114-byte JSON, one critical rate error, no data/child connection |

The native-safe projection contains `rate_message_marker` and `legacy_rate_code`,
with no scan truncation. The unchanged classifier returned `rate_limited`.
There was no Retry-After. This proves a GraphQL-level rate rejection over HTTP
200 for this attempt; it is not HTTP 429. The production fallback consequently
set the two-hour cooldown to **22:32:52.871886 UTC (6:32:52 p.m. Eastern)**.
It does not prove a permanent access restriction or a login requirement.

The actual child request used `PolarisPostChildCommentsQuery` /
`34884685271179117`, matching header/form names, `first=12`, logged-in provider
false, chronological false, and no cursor. Its media hash matches the numeric
media portion of the frozen photo's composite native ID; its parent hash matches
the preceding rejected child root. Authorization, proxy-authorization,
`sessionid` and `ds_user_id` were absent. `trust_env` and redirects were false.
The response SHA-256 is
`c6cd01e6d597b1d60b0baa2398ad005a96f0a1f1d0aa9be29bf295d72c01d44e`.
The native response now explains the runtime classification; sanitized historical
replay is not grounds to replace the classifier. Current remote query support
remains unproven because the request was rejected.

Post-run read-only verification against `vwxfvzutyufrkhfgoeaa` found all six
account runs cancelled, zero target owners, no pending control and no open budget
tranche. The original page receipt/hash and before/after observation hashes match.
All eleven canonical mappings remain present and distinct with zero identity
mismatches. The rolling ledger contains 67 charged requests, with 133 remaining.
The retry established neither parent continuation nor child retrieval/exhaustion.

An independent source investigator reviewed the safe HTTPX events and confirmed
the native rejection diagnosis. Scrapling's retained browser evidence still shows
only the logged-out dialog query, not successful pagination. No fresh Scrapling
source request was made during the renewed cooldown: the new evidence already
establishes the rejection, and another transport would introduce a separate
comparison. No later browser attempt or automatic retry is scheduled. Any future
comparison must preserve this run and the shared request limits; full history
remains gated. Decodo was not used in this retry.

Private evidence includes `httpx-retry-20261020.execution.log`, the safe observer
JSONL, preflight/post-run snapshots, original-state backup and started marker.
Only this validation record changed in the repository. No backend source, SQL,
app build, Modal deployment, secret or service configuration change was needed.

## Decodo setup repair and comparison preparation

The user authorized a bounded Decodo comparison of the same photo, carousel and
reel, including parent comments and nested replies, while keeping full history
gated. The scheduled direct retry had already completed as recorded above; its
result remains the direct baseline. It is not replayed by this comparison.

The standalone `trr-decodo-social-backfill` package had a missing package-local
helper and could report misleading readiness when MCP configuration was absent.
PLUGINS commit `9de8d3ae` repairs those defects, adds readiness/root-selection
regressions, and bumps the package to `1.0.1`. Five regression tests and six syntax
checks passed. Independent review of the complete package batch found no blocking
issue. A scoped refresh installed `1.0.1` with source/cache parity and preserved
listed-only registration and disabled MCP configuration. The installed dry-run
doctor reports eight passes and two warnings for absent managed API credentials
and MCP configuration; it does not claim an enabled managed API.

The user's existing Decodo dashboard was inspected in the verified Chrome `TRR`
profile. The configured proxy identity matched the selected TRR account without
exposing credentials. One provider-only proxy request returned HTTP 200 and JSON;
no raw response or egress address was retained. It was conservatively charged to
the shared ledger, bringing the observed total to 68 requests. No Instagram request
was made during setup. Existing residential allowance is sufficient for this
bounded test; no purchase, subscription change or new credential was needed.

The comparison uses explicit requests through the existing residential proxy so
each source request can be counted. Generic managed scraping API retries would
not provide the same source-request bound. The private runner must use the frozen
release's pure parsers, a new diagnostic identity, the existing account ledger and
local source gate, a single sticky anonymous proxy session, and a durable one-shot
marker. Limits are 24 source requests total, eight per post, ten minutes, 10 MiB of
decoded responses, zero automatic retries and no redirects. Stop the whole
comparison on rate/auth/provider rejection. Full history and the general worker
remain disabled.

One permalink per post supplies detail evidence and fresh bootstrap context. At
most two parent pages and two child pages for each of two freshly observed parent
IDs may follow. Inline reply previews do not count as child-page retrieval. Native
boolean pagination, cursor linkage and duplicate checks must distinguish genuine
exhaustion from a bounded partial result. Safe comparison evidence is private;
this diagnostic does not satisfy the production durable capability matrix.

The retained direct baseline has detail receipts for all three posts, one photo
parent page with eleven canonical mappings, and no retrieved child page. The
latest direct retry rejected the photo child root before reaching other comment
targets. These are retained observations from different times, not a simultaneous
controlled performance comparison. Decodo retrieval remains pending until its
reviewed runner executes after the live source gate permits it.

The private runner passed sixteen initial offline tests. Complete independent
review found native retry-delay handling and failure-time evidence-retention
defects; both were repaired with cross-page identity evidence in one batch. Eight
focused tests passed, followed by two affected retry tests after their last change.
Independent fix-only review found no remaining issue. The default plan command
confirmed no network or database access. These checks establish preparation, not
live retrieval success.

The one-shot app follow-up `instagram-decodo-bounded-comparison` is scheduled for
**October 20, 2026, 18:34 America/New_York (22:34 UTC)**. The driver separately
enforces the live gate, fixed source/state, shared budget and exclusive execution.
A started marker prevents repeating a started or uncertain attempt. A safe report
is retained on cleanup or final-verification failure; such a result is nonpassing.
The already-completed direct follow-up remains paused. No additional Instagram
request has been made during this preparation.

Browser verification used the user's existing vendor dashboard in Chrome `TRR`;
no TRR application startup target was needed. Backend/API production code, app
contracts and SQL are unchanged. TRR-APP build is not applicable. No Modal update
is needed for the private diagnostic and standalone plugin repair.

### Local limits and historical comparison

The 200-request rolling daily ceiling is a local fixed-validation budget introduced
by `98c6604b` on October 20, not an Instagram-published quota. No measured basis
for that specific ceiling has been established. The two-hour rejection fallback
is also local policy: the durable public lanes adopted it on October 19 from the
June public probe. The observed upstream response did not specify that duration.
The daily budget was not exhausted when Instagram rejected the child query.

The June 16 session record reports a public Modal sample with 33 detail results
and 823 of 1,208 advertised comments, without login, Decodo or a proxy. This is
historical reported evidence, not a current replay. Source at `cb5efbfe` confirms
the public configuration and HTTPX transport; a scraper class name did not mean a
rendered browser was used. Earlier authenticated successes are not a valid basis
for explaining away this public result.

Concrete differences remain to test: June parent-first requests and child fetches
shared one anonymous session, while the current resumed run starts pending child
work after a fresh bootstrap. The earlier default could skip unknown/zero reply
counts; strict durable validation probes pending unknown topology. Parent query
selection and local-versus-Modal egress also differ. The first child query ID is
unchanged. None of these source differences independently proves the cause of the
current rejection, and the old partial count does not establish complete nested
reply coverage. A clean-browser diagnostic is a separate transport experiment;
the current failing direct attempt was not a fresh browser window.

### User-directed direct browser test

The user then directed testing the same three posts without Decodo first and
explicitly rejected further scheduled tasks. The Decodo follow-up is paused;
the direct-retry and older canary follow-ups are also confirmed paused. No new
schedule is authorized for this test.

The immediate diagnostic uses a fresh temporary browser profile, no saved state,
no login and no proxy. It leaves the durable run and shared database gate unchanged.
For this explicitly requested isolated test, the unsupported local two-hour
fallback is not treated as a server instruction. The browser must stop on an
actual upstream rate rejection, login requirement or challenge. Source-data
requests use the existing ledger; static scripts have separate explicit caps.
This is a small diagnostic, not a resumption of full history or a change to the
production policy. A local-only startup check confirmed an empty initial cookie
jar and closed the temporary browser without making an Instagram request.

The first immediate browser attempt exposed a diagnostic mistake: its eight-data-
request per-post ceiling stopped normal initialization of the photo page. It made
eight document/XHR/fetch requests and sixteen script requests, verified the photo
detail identity, and clicked no controls. That was a local cap failure, not an
observed upstream rate rejection. Its started marker and result remain intact.

A corrected isolated attempt used a new output directory and retained all prior
ledger entries. Its private accounting override kept the shared stream lock and
tranche lifecycle, while using explicit diagnostic bounds of 100 data requests,
50 per post, 150 scripts, 75 per post, five minutes, and at most two comment and
two reply expansion clicks per post. The production accounting class and shared
source-gate row were not changed. Three focused correction tests passed; prior
unaffected synthetic checks and the successful browser launch were reused.

The corrected browser attempt reached the photo, verified its expected identity
and type, and clicked **Load more comments once**. It then stopped on the native
comment classifier's `source_auth_failed` result. There were 23 document/XHR/fetch
requests and 23 script requests; all 46 observed HTTP statuses were 200. The
rejected JSON body was 125 bytes, with one critical error and no child connection.
Its SHA-256 is
`fb43ffdc4b0a7a58f2c7c1d4919e0b88e038cbe181027483dfd7bbd0acd42cb3`.
There was no Retry-After or retained rate marker. No visible login form was
observed. The safe projection does not retain the exact authentication message;
this supports an authentication-related application rejection, not a proven
particular login requirement. No manually constructed GraphQL request was sent.
The failing event's query name is retained as `other`, so it cannot be attributed
to a specific parent or child query from this artifact. The classifier checks
recognized rate conditions before auth conditions; auth-classified does not prove
that Instagram's underlying internal reason could not involve throttling.

| Target | Fresh browser detail | Parent comment pages | Nested reply pages |
| --- | --- | --- | --- |
| `Dd37XeDly4U` photo | Verified | None; expansion caused rejection | None |
| `Dd60pnHl_ux` carousel | Not attempted after rejection | Not attempted | Not attempted |
| `Dd6m4XKDSQ1` reel | Not attempted after rejection | Not attempted | Not attempted |

The original run remained cancelled at generation 8 and passed final verification.
The account ledger contains 99 recorded requests and zero open tranches after
both diagnostic attempts. Temporary browser contexts were closed. No Decodo or
saved/login profile was used, no cooldown was imposed by this test, and all
Instagram follow-ups remain paused. Full history remains gated. These are actual
partial test results; neither all-three browser success nor comment/reply
retrieval has been established.

### Completed three-post browser diagnosis and corrected error attribution

The user explicitly requested immediate tests of the remaining carousel and reel
and the actual native error, rather than stopping at our classifier label. V3
visited both targets in independent fresh anonymous/no-proxy browser profiles.
It retained bounded, sanitized native error fields and actual operation names,
separated ancillary operation errors from comment/document failures, and allowed
a per-post access error to leave the other post test runnable. Eight distinct
focused checks passed with unaffected evidence reused. Parent review accepted the
changed paths before execution. No scheduled task was created or resumed.

The actual repeated error is **code 1675002, `Unauthorized logged out query.`**,
severity `CRITICAL`, returned over HTTP 200 by
`QuickPromotionSupportIGSchemaBatchFetchQuery`, document `26673487622279953`,
at `/api/graphql`. This is an ancillary promotion-support operation, not a
parent-comment or child-reply pagination operation. The 125-byte response hash
matches the previous photo failure. Hash equality identifies identical response
content but alone cannot recover the earlier request's operation name. A final
photo-only V4 diagnostic using corrected handling directly observed the same
promotion operation and native error, removing that attribution uncertainty for
the new photo attempt.

The earlier diagnostic's decision to stop the whole browser test on this ancillary
auth/access label was wrong. The native error establishes that this particular
operation rejects logged-out access; it does not establish a comment rate limit,
a login requirement for all public comments, or a failed comment-pagination call.
No matching public documentation for the internal error/query was found; the
captured request identity and native response supply the evidence here.

| Post | Verified native type | Source-reported comments | Source-reported likes | Corrected browser interaction |
| --- | --- | --- | --- | --- |
| `Dd37XeDly4U` | Photo, type 1 | 222 | 15,259 | Two load-more clicks; logged-out dialog query |
| `Dd60pnHl_ux` | Carousel, type 8 | 34 | 1,319 | Login-route document request observed; no completed load-more click |
| `Dd6m4XKDSQ1` | Reel/video, type 2 | 96 | 3,153 | Two load-more clicks; logged-out dialog query |

These are fresh detail metadata counts, not retrieved comment rows. All three
detail identity/type checks passed. Photo and reel each returned HTTP 200 for
`PolarisLoggedOutDynamicDialogQuery` (133-byte body), recorded one visible dialog,
and showed no visible username/password form. Carousel recorded `redirect_login`,
but the request frame identity was not retained, so this does not establish a
top-level login wall. No recognized native parent or child pagination connection
was returned by the inspected responses. Supported inline/preview shapes were
also inspected separately; absence of a recognized shape is not proof that every
possible bootstrap or DOM comment format was absent.

This explains the current zero-page metric: it counts recognized actual comment
pagination, while these browser interactions produced ancillary/logged-out UI
operations. The historical June public HTTPX path explicitly sent parent-comment
queries, then children; it was not this rendered-browser interaction path. The
older substantial public retrieval remains valid historical evidence. Current
browser results do not by themselves prove Instagram globally disabled anonymous
comment retrieval. The separate earlier HTTPX child-root rate error remains a
different, independently observed failure.

V3 used 56 data requests and 52 scripts; V4 photo used 28 data requests and 25
scripts. Both diagnostics completed and final guards verified the frozen run
unchanged. Comments/replies retrieval did not pass. The shared ledger now records
183 requests in the rolling window and zero open tranches. Temporary contexts
closed, the shared gate was not modified, no new cooldown was imposed, and all
Instagram schedules remain paused. Full history stays gated. Safe records are
`direct-browser-20261020-v3/result.json` and
`direct-browser-20261020-v4-photo/result.json` under the existing private task base.
