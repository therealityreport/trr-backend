# TRR Backend

This context defines TRR backend domain language for social ingestion and related data products.

## Language

**Complete Instagram Post Snapshot**:
A TRR-native capture of stable, account-independent Instagram post data, including public post fields, owner fields, caption, engagement counts, media variants and carousel children, tagged users, collaborators, location/music/ad flags where available, hosted media, full comments and replies, and comment metadata such as author avatar URL, username, verification status, likes, reply counts, and comment media. It excludes session-dependent facts about the account used to scrape, such as whether that account liked, saved, can reshare, or has mutual-follow relationships with the post owner.
_Avoid_: Apify snapshot, raw Instagram dump, viewer session state

**Instagram Backfill Runtime Target**:
The performance target for Instagram scraping measured primarily by how long a complete account/date-window backfill takes, with per-post comment and detail timing tracked as secondary guardrails so unusually large posts do not hide inside the account-level average.
_Avoid_: Per-post-only target, synthetic speed score

**Adaptive Instagram Scrape Speed**:
The scraper's preferred operating mode: finish the account/date-window backfill as fast as possible while automatically backing down when Supabase pressure, proxy health, Instagram rate-limit signals, auth challenges, retry volume, or failed writes show elevated risk.
_Avoid_: Fixed slow rate, unbounded max-speed scrape

**Lane-Specific Scrape Backoff**:
An adaptive slowdown that applies only to the stressed Instagram scrape lane, such as posts, comments, media mirror, or database writes, while unrelated healthy lanes may continue bounded work. Auth or challenge signals stop the affected Instagram identity immediately instead of retrying through the same identity.
_Avoid_: Whole-run pause, blind retry loop

**Instagram Partial Success with Retry Queue**:
The completion model for Instagram backfills where valid scraped data is saved immediately, missing snapshot parts are recorded as explicit retry targets with cursors or checkpoints, and an account/date-window is complete only when each required part is either captured or explicitly unavailable from Instagram.
_Avoid_: All-or-nothing run, silent partial completion

**Instagram Source-Unavailable Evidence**:
A stable Instagram source signal that proves a required snapshot part cannot be captured, such as disabled comments, deleted media, private or restricted access, an unavailable media URL, exhausted comment pagination, or a known object-inaccessible response. Timeouts, 429s, proxy failures, and auth failures are operational failures and remain retryable.
_Avoid_: Inferred unavailability, retry exhaustion as source truth

**Adaptive Scrape Control Plane**:
A shared service that decides current scrape pressure from Supabase health, Instagram and proxy risk, retries, active jobs, and write failures, then publishes simple lane budgets such as normal, reduced, paused, and identity-blocked. Individual lanes enforce those budgets in lane-specific ways for comments, posts, media mirror, and database writes.
_Avoid_: Per-lane pressure policy, duplicated backoff logic

**Instagram Lane Budget**:
The current operating allowance for one scrape lane, expressed as normal, reduced, paused, or identity-blocked, with precedence for blocked identities, proxy cooldowns, account-lane pauses, global lane budgets, and defaults.
_Avoid_: Worker speed setting, retry limit

**Hosted Snapshot Completion**:
The media-completeness rule for Instagram snapshots where source URLs alone are partial and hosted media, comment media, and avatar mirrors must be completed or marked source-unavailable before the snapshot is complete.
_Avoid_: Source URL completion, best-effort media

**Instagram Mega-Post Shard**:
A one-post retry or scrape job for posts whose expected comment volume or runtime is large enough that they should not share a batch with ordinary posts.
_Avoid_: Large batch item, hidden slow post
