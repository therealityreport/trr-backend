# TRR Media Scraping Source Coverage Matrix

This matrix covers the first implementation slice: WWHL show/person image runs. Social media mirror sources are listed only as related media and are not part of this slice.

| Source | Owner module | Entity coverage | Image role | Primary identifiers | Acquisition path | Scrapling use | Display eligibility | Review risks and limits |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Getty search/detail pages | `trr_backend.integrations.getty` | show, season, episode by event text, person by caption/people metadata | metadata, reference preview, watermarked preview | Getty editorial id, NUP filename, NUP set, source page URL | HTML search/detail discovery | `Selector` for fragile detail parsing; `StealthyFetcher` only for blocked discovery | Not eligible by default; reference-only unless attached to approved non-watermarked replacement | Public images are watermarked; page access can be challenged; metadata may be ambiguous |
| NBCUMV GraphQL | `trr_backend.integrations.nbcumv` | show, season, episode, person by caption/search filters | press image metadata and thumbnails | `lbx_id`, `lbx_filename`, NUP filename, show id, live date | AppSync GraphQL | None by default; keep structured API path | Eligible when paired with hi-res or valid hosted URL | API schema or public key behavior may change |
| NBCUMV CloudSearch | `trr_backend.integrations.nbcumv` | show, episode, person discovery | metadata lookup and fallback discovery | item number, filename, thumbnail, metadata fields | CloudSearch HTTP API | None by default; keep structured API path | Not directly; use to find canonical NBCUMV records | Result ranking can be noisy; does not expose direct full-res URL |
| NBCUMV batch ZIP download | `trr_backend.integrations.nbcumv` | image asset acquisition for NBCUMV records | full-resolution original | `lbx_id`, `lbx_filename` | batch download API, presigned ZIP | None by default; keep structured HTTP path | Preferred full-resolution source when successful | Presigned URL can expire; endpoint behavior can change |
| Bravo JSONAPI | `trr_backend.integrations.bravo_jsonapi` | show, season, episode, person/gallery context | editorial image, gallery context, lower-res web crop | media UUID, file UUID, file URL, gallery item id, gallery path | Drupal JSONAPI | None for JSON; `Selector` only for HTML fallback | Eligible when hosted/image quality fields are valid; otherwise editorial context | Not complete press gallery coverage; may lack full dimensions |
| Bravo page HTML fallback | `trr_backend.integrations.bravo_jsonapi` | gallery item anchoring and metadata fallback | source page context | gallery path, gallery item id, file URL | HTML page fetch | `Selector` can replace brittle regex/BeautifulSoup fallback | Same as Bravo JSONAPI after quality checks | DOM can change; fallback must not override JSONAPI truth |
| IMDb supplemental person images | `trr_backend.ingestion.cast_photo_sources` / related ingestion modules | person | supplemental gallery image | IMDb person/title ids, media URLs | existing integration/scraper | Out of scope for first slice | Eligible only through existing supplemental import rules | Not source of WWHL press originals |
| TMDb supplemental person images | `trr_backend.ingestion.tmdb_person_images` | person | supplemental profile/gallery image | TMDb ids, image paths | TMDb API | Out of scope for first slice | Eligible through existing TMDb source policy | API coverage is curated and not episode-specific |
| Fandom supplemental person images | `trr_backend.ingestion.fandom_person_scraper` | person | supplemental gallery/confessional image | page URL, file URL, inferred context | existing scraper | Out of scope for first slice unless later converted | Eligible through existing Fandom source policy | Fan wiki labeling can be inconsistent |
| Existing social media mirrors | social scraper/media mirror modules | social post, comment media, related cast/show media | related media, not BRAVOTV press/gallery media | platform ids, post ids, media URLs, mirror job ids | existing social scraper pipeline | Separate Scrapling lanes already exist for social platforms | Not part of WWHL image run source precedence | Do not mix into first WWHL show/person image slice |

## Source Precedence

1. NBCUMV hi-res original.
2. Hosted Bravo editorial image when quality fields are valid.
3. Approved public replacement.
4. Getty reference preview for review only.

Getty reference previews must not silently become primary display media.
