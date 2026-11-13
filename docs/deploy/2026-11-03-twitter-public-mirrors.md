# Anonymous X mirror deployment and validation

Target: `admin-56995 / main / trr-backend-jobs`.
App: `ap-DkLTRoSvqhbkGO7fHyHxrD`.

The release includes the opt-in FxTwitter conversation collector and a bounded
Lurkbird public-search adapter. Neither collector loads X credentials, Apify
credentials, imported cookies, browser profiles, proxy configuration, or paid
scraping services. Existing Modal operator access is used only for deployment
and runtime verification. Existing TRR infrastructure costs still apply.

## Scope and entry points

- `public_mirror.py`: known-post conversation pages, cursor traversal, parent IDs.
- `public_search.py`: explicitly selected Lurkbird query, at most two pages and
  40 records, no retries. Requests use a new session with `trust_env=False`, no
  redirects, no cookie import, finite time and response-size limits.
- `public_probe.py`: small state-free deployment checks.
- Modal `probe_twitter_public_conversation`: two pages/40 records, 120 seconds.
- Modal `probe_twitter_public_search`: one page/10 records, 60 seconds.
- Both probes have no attached secrets, no schedules, zero retries, and at most
  one container. They do not persist records or download media.

The existing authenticated API/search defaults have not been switched. These
were initially explicit Python/Modal entry points. The later opt-in
`anonymous_research` mode is also available through the existing protected
Twitter search endpoint and its app consumer. It does not create a public
unauthenticated endpoint or persist research results.

```python
from trr_backend.socials.twitter import search_public_posts, fetch_public_conversation

search = search_public_posts(
    '"Cat\'s Head Nebula"', provider="lurkbird", max_pages=1, max_items=10
)
conversation = fetch_public_conversation("2117633046002225363", max_pages=2, max_items=40)
# A discovered reply ID can also be used to inspect that branch.
branch = fetch_public_conversation("2117681802089517504", max_pages=1, max_items=30)
```

Lurkbird's [terms](https://lurkbird.com/terms/) allow personal use and prohibit
automated bulk scraping. This adapter is for small, explicit research reads.
Do not attach it to scheduled ingestion or use it as a bulk-search fallback.
The site's [robots file](https://lurkbird.com/robots.txt) allows public pages but
excludes downloads and internal refresh routes; this adapter uses neither.

## Verified capabilities and limits

| Method | Evidence | Remaining limits |
| --- | --- | --- |
| Global keyword search | `Artemis` returned 18 records; quoted Cat's Head Nebula query returned 22 records in investigation. | Provider-selected subset; freshness, ranking and global coverage unknown. |
| Search pagination | Clean-environment adapter returned 40 unique NASA IDs, 20 newly accepted per page. | Two-page research limit; no complete timeline claim. |
| Conversation pagination | Three bounded root pages returned 74 unique posts. | Provider cursor exhaustion does not establish completeness. |
| Nested branches | Two selected reply branches each yielded one additional post absent from the root result. | Branch discovery is bounded; missing/hidden branches remain unknown. |
| Anonymous first-party verification | Public X syndication responses confirmed search records and two newly discovered replies. | This checks public extraction, not equivalence with any paid Actor. |

Search record `2117814845315891536` matched first-party text, author and timestamp
exactly. NASA record `2117633046002225363` matched author and timestamp; displayed
text matched after removing only the official response's media-entity URL and
right-stripping whitespace. Raw responses and this normalization rule are kept
in the evidence. The adapter labels text as mirror display text because it may
omit media links or truncate long posts; it does not claim full source text.

Additional branch records `2117697681711923359` and `2117810282978578707` matched
first-party text, author and `in_reply_to_status_id_str`. The confirmed parents
were `2117697122208555440` and `2117681802089517504`, respectively.

Full threads remain **unknown**. All results preserve `complete=False`.
The mirrors' upstream authentication is unknown; no client credentials are sent.
Lurkbird's known-post page returned only the root, so it is not used for replies.
The investigated xitter.cf service explicitly requires a search challenge and
reports server-side account pools; no challenge or authentication was attempted.
Previously failing FxTwitter search probes were not repeated without a new mechanism.

## Validation and release

- 15 focused search tests pass: recorded parser fixture, cursor encoding,
  pagination, new IDs, deduplication, repeated pages, partial failure, invalid
  bounds, blocked/empty pages, off-origin continuation rejection, and no ambient
  auth/proxy or redirects.
- Six deployment-probe tests pass; prior unchanged conversation regression
  evidence remains applicable.
- Ruff and scoped diff checks pass.
- v181 conversation runtime returned 39 replies with no error and zero calls to
  the authentication loader. Its collector hashes matched the release.
- The first deployed search check exposed equivalent percent-encoding in an
  apostrophe-containing query. Commit `92fa1ae1` fixes decoded-path comparison
  while preserving exact HTTPS origin and query/sort checks. Its regression passes.
- Final deployment **v182**, tag `twitter-public-92fa1ae1`, passed the guarded
  readiness check, maintenance ownership/schedule check and HTTP 200 API canary.
  The deployed search probe returned ten records, no error, and `max_items`.
  Its NASA result matched first-party author, timestamp and display text after
  the documented media-link normalization.
- All three collector hashes matched release source. The unchanged conversation
  collector reuses v181's successful runtime evidence. Provider metadata confirms
  zero secrets and no schedule for both final probe functions.

Release source is exported from committed Git content, excluding all unrelated
working-tree edits. Prior deployment v180 remains the pre-change rollback.
Source changes: `39a1d446` (conversation), `bbef3626` (probe), `24a783f2` (search),
`92fa1ae1` (continuation fix). No production data, secrets, active chats or browser
sessions were changed. No browser was used. TRR-APP build is not applicable.

Operator evidence is retained outside source:
`~/.local/share/trr-twitter-validation/20261103/` (release manifest, deployment
logs, provider metadata, remote results, root/branch data and independent checks),
and `~/.local/share/last30days-mirror-validation/deploy-followup/` (new search
investigation, source pages and first-party comparisons).
The minimal search fixture is derived from the recorded public `Artemis` page;
unrelated page scripts, analytics, icons and controls were removed.

The disposable release export is retired after verification; reconstruct it with
`git archive 92fa1ae13cd43acfb1fcff61e1cad9c58a20286d`. Git commits and evidence
remain retained. The previous deployment v180 is still listed by the provider;
rollback was not exercised against production. Its supported command, if needed,
is `python -m modal app rollback ap-DkLTRoSvqhbkGO7fHyHxrD v180 --env main --profile admin-56995 --strategy rolling`.

## November 4 integration: branch expansion

`trr_backend.socials.twitter.public_thread.expand_public_thread` adds an explicit
`provider="fxtwitter"` option for bounded branch exploration. One invocation has
at most five provider requests and 100 unique output posts. It reads up to two
root pages, then one page from selected discovered replies, retaining IDs,
parent relationships, branch receipts and unvisited continuations. It stops on
provider errors and keeps `complete=False`; reply counts prioritize exploration
but do not prove missing branches empty.

The clean-environment live run returned 67 unique posts in five requests: 62
from two root pages, followed by 1, 1 and 3 additional posts from three branches.
First-party responses independently confirmed text, author and parent IDs for
`2117661883193057454` and `2117809213321650308`; earlier verified branch evidence
remains available. Seven focused fixture tests passed; after adding explicit
partial/unavailable status, the two affected tests were rerun separately.
Evidence: `~/.local/share/trr-twitter-validation/20261104/`.
