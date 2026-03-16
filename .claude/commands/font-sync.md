# /font-sync — Monotype Font Pipeline

Collect fonts from Monotype cache, stage on Desktop, upload to R2, register in web app, and verify rendering.

**Full instructions are in:** `/Users/thomashulihan/Projects/TRR/skills/font-sync/SKILL.md`

Read that file first, then execute the 5-step pipeline:

## Pipeline

1. **Collect** — Find font files in `~/Library/Application Support/Monotype Fonts/Monotype Fonts_d74c9132-777f-46eb-9b37-263fce1b0ed1/.Fonts/`, copy to `~/Desktop/FONTS/<Family>/`
2. **Upload** — Push to R2 bucket `trr-media-prod` via `python3.11 TRR-APP/scripts/upload-fonts-to-s3.py`
3. **CSS** — Add `@font-face` rules to `TRR-APP/apps/web/src/styles/cdn-fonts.css` (alphabetical order)
4. **Register** — Add `CDN_FONTS` entry in `TRR-APP/apps/web/src/components/admin/design-system/DesignSystemPageClient.tsx` (alphabetical order)
5. **Verify** — Confirm fonts actually render (not falling back to system fonts):
   - `curl -sI` every font URL to confirm HTTP 200
   - Cross-check `font-family` name is identical in `cdn-fonts.css` and `fontFamilyValue`
   - Run `document.fonts.check('16px "<Family>"')` in browser console — must return `true`
   - Visually confirm font card on `/admin/fonts` renders in the correct typeface

## Arguments

If the user provides font family names, process those. Otherwise ask which families to sync.

## Critical: Prevent fallback

The `font-family` string must be **character-identical** in:
- `cdn-fonts.css` `@font-face` rule
- `DesignSystemPageClient.tsx` `fontFamilyValue` field
- The actual font file must exist at the R2 URL (case-sensitive, spaces as `%20`)

Any mismatch causes silent fallback to system fonts with zero errors.
