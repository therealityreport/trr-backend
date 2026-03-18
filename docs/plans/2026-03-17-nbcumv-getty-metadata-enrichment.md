# NBCUMV/Getty Metadata Enrichment Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Populate top-level metadata keys (show, season, episode, created_at, people_count, people_names, content_type) during NBCUMV and Getty import so the frontend can display them without manual admin tagging.

**Architecture:** The backend stores rich metadata from Getty/NBCUMV in nested objects (`metadata.nbcumv.*`, `metadata.getty.*`) but the frontend reads top-level keys like `metadata.show_name`, `metadata.season_number`, `metadata.people_count`. This plan adds extraction/aliasing in the two row-building functions: `_build_asset_metadata()` (NBCUMV path) and `_build_getty_cast_photo_row()` (Getty-only path). Also fixes a bug where "One Person" tags don't set people_count.

**Tech Stack:** Python 3.11, FastAPI, Supabase, pytest

---

### Task 1: Fix `_infer_people_count` to handle singular "person"

**Files:**
- Modify: `trr_backend/integrations/getty.py:769-780`

**Step 1: Write the fix**

The regex `([a-z]+)\s+people` doesn't match "One Person" (singular). Add "person" patterns:

```python
def _infer_people_count(keyword_texts: list[str]) -> int | None:
    for raw_value in keyword_texts:
        lowered = str(raw_value or "").strip().lower()
        if not lowered:
            continue
        word_match = re.fullmatch(r"([a-z]+)\s+(?:people|person)", lowered)
        if word_match:
            return _PEOPLE_COUNT_WORDS.get(word_match.group(1))
        number_match = re.fullmatch(r"(\d+)\s+(?:people|person)", lowered)
        if number_match:
            return int(number_match.group(1))
    return None
```

**Step 2: Run existing Getty tests**

Run: `cd TRR-Backend && python -m pytest tests/ -k getty -q`

**Step 3: Commit**

---

### Task 2: Enrich `_build_asset_metadata()` with top-level keys (NBCUMV path)

**Files:**
- Modify: `api/routers/admin_nbcumv.py:373-434`

**Step 1: Add extraction helpers before `_build_asset_metadata()`**

```python
def _parse_int_field(image: dict, *keys: str) -> int | None:
    for key in keys:
        raw = image.get(key)
        if isinstance(raw, int):
            return raw
        if isinstance(raw, str) and raw.strip().isdigit():
            return int(raw.strip())
    return None


def _parse_episode_number_from_caption(caption: str | None) -> int | None:
    if not caption:
        return None
    match = re.search(r"Episode\s+(\d+)", caption, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None
```

**Step 2: Add top-level keys to `_build_asset_metadata()` payload**

After the existing `payload = { ... }` dict on line 413, before the gallery_bucket merge on line 431, add:

```python
    # --- Top-level keys the frontend reads directly ---
    # Show
    if isinstance(gallery_bucket, dict):
        if gallery_bucket.get("resolved_show_name"):
            payload["show_name"] = gallery_bucket["resolved_show_name"]
        if gallery_bucket.get("resolved_show_id"):
            payload["show_id"] = gallery_bucket["resolved_show_id"]

    # Season
    season_number = _parse_int_field(image, "lbx_seasonNumber", "lbx_season")
    if season_number is not None:
        payload["season_number"] = season_number

    # Episode
    episode_number = _parse_int_field(image, "lbx_episodeNumber")
    if episode_number is None:
        episode_number = _parse_episode_number_from_caption(image.get("lbx_caption"))
    if episode_number is not None:
        payload["episode_number"] = episode_number

    # Episode title
    episode_title = str(image.get("lbx_episodeTitle") or "").strip() or None
    if episode_title:
        payload["episode_title"] = episode_title

    # Created date (frontend reads created_at, not published_at)
    if published_at:
        payload["created_at"] = published_at

    # People count from Getty or length of tagged people
    if isinstance(getty, dict) and getty:
        from trr_backend.integrations.getty import _infer_people_count as _getty_infer_people_count
        getty_pc = getty.get("people_count")
        if isinstance(getty_pc, int) and getty_pc >= 0:
            payload["people_count"] = getty_pc
        elif isinstance(getty_pc, str) and getty_pc.strip().isdigit():
            payload["people_count"] = int(getty_pc.strip())
        elif getty_tags:
            inferred = _getty_infer_people_count(getty_tags)
            if inferred is not None:
                payload["people_count"] = inferred
    if "people_count" not in payload and tagged_people:
        payload["people_count"] = len(tagged_people)

    # People names (frontend reads people_names, not tagged_people)
    if tagged_people:
        payload["people_names"] = list(tagged_people)

    # Content type at top level (frontend reads metadata.content_type)
    nbcumv_content_type = nbcumv_enriched.get("content_type") or nbcumv_enriched.get("lbx_type")
    if isinstance(nbcumv_content_type, str) and nbcumv_content_type.strip():
        payload["content_type"] = nbcumv_content_type.strip()
```

**Step 3: Run existing tests**

Run: `cd TRR-Backend && python -m pytest tests/api/routers/ -q`

**Step 4: Commit**

---

### Task 3: Enrich `_build_getty_cast_photo_row()` metadata (Getty-only path)

**Files:**
- Modify: `api/routers/admin_person_images.py:1204-1269`

**Step 1: Add season/episode/date extraction to the Getty-only row builder**

After the existing metadata dict construction (line 1244), before the return statement (line 1252), add:

```python
        # --- Season from Getty tags ---
        season_number = None
        for tag in metadata.get("getty_tags") or []:
            tag_match = re.search(r"\bSeason\s+(\d+)\b", str(tag), re.IGNORECASE)
            if tag_match:
                season_number = int(tag_match.group(1))
                break
        if season_number is not None:
            metadata["season_number"] = season_number

        # --- Episode from caption ---
        caption_text = str(asset.get("caption") or "").strip()
        ep_match = re.search(r"Episode\s+(\d+)", caption_text, re.IGNORECASE)
        if ep_match:
            metadata["episode_number"] = int(ep_match.group(1))

        # --- Created date ---
        date_created = str(asset.get("date_created") or "").strip()
        if date_created:
            metadata["created_at"] = date_created

        # --- People names at top level ---
        if people:
            metadata["people_names"] = people
```

Also add `season` to the return dict:

```python
        return {
            ...
            "season": season_number,  # ADD this column
            ...
        }
```

**Step 2: Run tests**

Run: `cd TRR-Backend && python -m pytest tests/api/routers/test_admin_asset_flags.py -q`

**Step 3: Commit**

---

### Task 4: Verify end-to-end

**Step 1:** Run full test suite: `cd TRR-Backend && python -m pytest tests/ -q --tb=short`

**Step 2:** Syntax check all modified files:
```bash
python -c "import py_compile; py_compile.compile('trr_backend/integrations/getty.py', doraise=True)"
python -c "import py_compile; py_compile.compile('api/routers/admin_nbcumv.py', doraise=True)"
python -c "import py_compile; py_compile.compile('api/routers/admin_person_images.py', doraise=True)"
```

**Step 3:** Commit all changes together
