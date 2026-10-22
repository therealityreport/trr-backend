# Native GIF delivery repair and bounded anonymous acceptance

Status: GIF repair committed, independently reviewed, deployed and installed.
Bounded anonymous acceptance has not passed: the first January listing activity
returned HTTP 401. Full-history stays disabled.

## Repair

Backend commit `72444c9f` preserves only the reviewed CDN delivery query keys for
exact host `cdn.fbsbx.com` during evidence capture. It preserves signature encoding
and ordering while removing account credentials and URL fragments. Parent,
sibling and look-alike hosts do not receive this exception. Existing deployed
attachment/authentication evidence fields remain intact in the release candidate.

The new offline regression follows evidence capture/read through the production
comment media selector into ordinary media hosting. It verifies absent comment
text, original animated bytes, and one immutable object write across repeated
hosting. It does not establish fresh anonymous retrieval or canonical run
completion.

Validation: the regression failed before the repair because the delivery query
was removed. After the repair, 119 affected evidence/media tests passed in 12.47
seconds. Scoped Ruff lint and formatting passed. Independent complete-batch review
reported no actionable findings; exact native instruction matching is unverified
because the host encrypts the child input. A state-free source smoke passed GIF
selection, credential/host boundaries, anonymous default and full-history closure.

## Coordination and acceptance

The user authorized coordination with the existing January execution and release
owners. One integrated rollout must preserve this repair and the concurrently
owned cooldown-policy change. This task does not independently deploy a stale
candidate or operate a second January controller.

The read-only audit at 07:14 UTC found bounded public/local January run
`c5880efe-25cf-4aeb-999a-efc36a0b9701` queued at generation 5, with zero listing
source requests and an unresolved root target. The immutable window is January 1
inclusive through February 1 exclusive, 2026 UTC. The later policy rollout owns
any supported transition of its existing wait; this audit is historical state,
not a fresh post-rollout observation.

Acceptance remains to verify source-backed older listing, exact parent/reply ID
inventories and pagination exhaustion, truthful unresolved gaps, anonymous
post-detail/media variants, hosted attachments and idempotent canonical writes.
The already verified recovery/display of the two original GIFs is reused and does
not satisfy fresh anonymous pipeline coverage. See the [readiness record](2026-10-22-instagram-anonymous-readiness.md).

Private evidence: `/Users/thomashulihan/.codex/tmp/trr-gif-anonymous-01a1626d/`.
The GIF repair changes no schema or app source. TRR-APP build is not applicable.

## Delivered runtime and first source check

The GIF repair first reached Modal v116, then was preserved in the v117 API
input-concurrency repair and v118 (`reddit-periods-674862fe`). Target:
`admin-56995 / main / trr-backend-jobs`. The accepted source identity is
`ig-no-local-cooldown-api-e60990617c12`; the adopted manifest contains 2,645 files.
The exact deployed GIF evidence module hash matches the reviewed candidate.
The state-free live GIF smoke passed all signature, credential, host-boundary,
anonymous-default and full-history-closure assertions. The 59-module runtime
proof passed, with unchanged module evidence reused across the disjoint API and
Reddit updates. The deployment wrapper passed readiness and the API canary;
separate `/health` and `/health/live` checks returned HTTP 200.

The matching Mac release is installed under that same source identity. The prior
verified `ig-public-lane-097e66e1-ab4f2feec7ba` release remains for rollback. The
superseded older copy was dependency-checked and moved to actual Trash, leaving
two runnable copies. Provider recovery uses retained exact frozen source.

The first production January activity completed at a clean activity boundary:
two counted listing requests, followed by HTTP 401 on the GraphQL POST. Its target
is unresolved (`public_access_backoff`); the code name does not imply a new local
cooldown. No January discovery pages, comments pages or commit receipts exist.
Generation 5, intent hash and checkpoint are preserved. The worker stopped with
zero active owners and zero uncertain dispatches; no unchanged rejection loop
was started. This is a verified failed source attempt, not successful discovery.

The bounded same-contract HTTP-client comparison also returned HTTP 401 after
at most two requests. Changing only the client transport did not qualify a
production default change.
No source-family, query-ID or cursor substitution is accepted from this report.
The original GIF recovery and the offline repair do not prove fresh anonymous
GIF source availability or complete January media persistence.

## Offline source-contract finding

The retained `PolarisOwnerToTimelineMediaLoggedOutQuery` (29457667847192124) and
continuation (28161631910182549) are used by a caller explicitly gated to
`polaris.postPage`. Their owner/media filters and chaining response cannot prove
chronological profile history. They are not a qualified replacement for the
frozen January profile-listing family. Earlier logged-out profile-query names
were regex discoveries in loaded code, not observed successful profile requests.

## Bounded fresh anonymous profile observation

The execution owner ran a fresh ephemeral headless browser with no authenticated
state, proxy or user profile. Approved native requests passed through the public
source gate, coordinator lease and account budget. Limits remained six document/API
requests, 80 static JavaScript requests and 90 seconds. No canonical or January
control writes were permitted. Synthetic fixtures verified route bounds, identity
rejection and delayed profile-response observation without external traffic.

The corrected observer waited up to 18 seconds for profile readiness. It approved
two document/API requests and 19 static JavaScript requests; all 21 responses
returned HTTP 200. It observed a native logged-out story-highlights query, no
profile-post query or post links, and a persistent loading indicator. It found no
explicit login dialog, private-profile message or unavailable-profile message.
Seventy-seven other requests were blocked under the existing allowlist. This
result does not establish an empty profile or prove source absence.

Blocked metadata included `fetchPolarisLoggedOutExperimentQuery`
(`26916974064594035`). Retained code verifies a read-only query, but its inspected
callers launch exposure/feature-wall requests without awaiting the result before
returning existing feature settings. Other inspected callers concern mobile
bio-links or displaying a logged-out call to action. The evidence does not
establish a required profile-timeline dependency. The observer was not broadened
and no synthetic dependency was invented from that assumption.

Fresh anonymous listing qualification remains inconclusive. Exact accepted reply
IDs, productive pagination and the previously recovered original GIF bytes remain
valid prior evidence; they do not supply missing January discovery, anonymous
attachment retrieval or idempotent January canonical commit receipts. Full-history
ingestion remains disabled until all acceptance gates pass.

Private current evidence references:

- `trr-instagram-no-local-cooldown-01a16023/live-gif.json` and `live-core.json`.
- `trr-instagram-no-local-cooldown-01a16023/release-api/api-closure.json`.
- `trr-instagram-no-local-cooldown-01a16023/release-current/manifest.json` and
  `preservation.json`.
- `trr-instagram-settlement-01a16023/headless-public/first-january-activity-result.json`,
  `after-first-january-activity.json` and `integrated-retirement.json`.
- This task's `acceptance-hints.json`, `logged-out-contract-reconstruction.json`
  and `same-family-transport-assessment.json`.
- `trr-instagram-settlement-01a16023/headless-public/anonymous-profile-browser-qualification.json`
  and its preserved `early-observation-1` predecessor.
- This task's `experiment-dependency-assessment.json` records retained caller
  locations, source hashes and the limits of the offline dependency finding.

All paths above are under `/Users/thomashulihan/.codex/tmp/`.
