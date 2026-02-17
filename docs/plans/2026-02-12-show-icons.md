# Show Icons Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Store show icons in S3 under `icons/{show_key}/` (alongside `fonts/`), with backend upload/list/delete API and frontend upload UI in both a dedicated icon library and the ShowBrandEditor.

**Architecture:** A simplified direct-upload flow (server receives bytes, writes to S3, returns CDN URL). No presigned POST complexity needed since icons are small admin-only uploads. The backend stores icon metadata in a new `show_icons` table. Frontend gets a reusable `IconUploader` component used in both the ShowBrandEditor inline slot and a browsable icon library page.

**Tech Stack:** FastAPI (backend), boto3/S3, Supabase/PostgreSQL, Next.js 14 App Router (frontend), React, TailwindCSS

---

## Architecture Decision: Why Not Reuse `user_uploads.py`?

The existing presigned POST flow (`user_uploads.py`) stores files at content-addressed `media/{sha256[:2]}/{sha256}{ext}` paths. The user specifically wants icons stored at human-readable `icons/{show_key}/{filename}` paths alongside `fonts/`. A direct upload flow is simpler, fits the admin-only use case, and matches the `fonts/` upload pattern (see `upload-fonts-to-s3.py`).

---

### Task 1: Add `build_icon_s3_key()` to `s3_mirror.py`

**Files:**
- Modify: `trr_backend/media/s3_mirror.py` (after `build_logo_s3_key` ~line 378)
- Test: `tests/media/test_s3_mirror_icons.py`

**Step 1: Write the failing test**

```python
# tests/media/test_s3_mirror_icons.py
"""Tests for icon S3 key builder and prefix helper."""

from trr_backend.media.s3_mirror import build_icon_s3_key, get_icon_s3_prefix


def test_build_icon_s3_key_basic():
    key = build_icon_s3_key(show_key="real-housewives-of-beverly-hills", filename="logo.png")
    assert key == "icons/real-housewives-of-beverly-hills/logo.png"


def test_build_icon_s3_key_sanitizes_filename():
    key = build_icon_s3_key(show_key="rhobh", filename="My Icon (1).png")
    # Filename should be sanitized: lowercase, no spaces or parens
    assert key.startswith("icons/rhobh/")
    assert " " not in key
    assert "(" not in key


def test_build_icon_s3_key_preserves_extension():
    key = build_icon_s3_key(show_key="rhobh", filename="icon.svg")
    assert key.endswith(".svg")


def test_get_icon_s3_prefix():
    prefix = get_icon_s3_prefix("rhobh")
    assert prefix == "icons/rhobh/"
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m pytest tests/media/test_s3_mirror_icons.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_icon_s3_key'`

**Step 3: Write minimal implementation**

Add to `trr_backend/media/s3_mirror.py` after `build_logo_s3_key` (after line 378):

```python
def _sanitize_icon_filename(filename: str) -> str:
    """Sanitize an icon filename: lowercase, replace spaces/special chars with hyphens."""
    if not filename:
        return "icon"
    # Split name and extension
    parts = filename.rsplit(".", 1)
    name = parts[0]
    ext = f".{parts[1].lower()}" if len(parts) > 1 else ""
    # Lowercase and replace spaces/underscores/parens with hyphens
    slug = name.lower().strip()
    slug = re.sub(r"[\s_()]+", "-", slug)
    slug = re.sub(r"[^a-z0-9\-.]", "", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    return f"{slug or 'icon'}{ext}"


def build_icon_s3_key(show_key: str, filename: str) -> str:
    """
    Build S3 key for show icons.

    Path: icons/{show_key}/{sanitized_filename}

    Args:
        show_key: The show's URL-safe key (e.g., 'real-housewives-of-beverly-hills')
        filename: Original filename (will be sanitized)
    """
    sanitized = _sanitize_icon_filename(filename)
    return f"icons/{show_key}/{sanitized}"


def get_icon_s3_prefix(show_key: str) -> str:
    """
    Build the S3 prefix for a show's icons.

    Returns:
        S3 prefix like "icons/rhobh/"
    """
    return f"icons/{show_key}/"
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m pytest tests/media/test_s3_mirror_icons.py -v`
Expected: PASS (all 4 tests)

**Step 5: Commit**

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git add trr_backend/media/s3_mirror.py tests/media/test_s3_mirror_icons.py
git commit -m "feat: add build_icon_s3_key() and get_icon_s3_prefix() to s3_mirror"
```

---

### Task 2: Create `show_icons` Database Table

**Files:**
- Create: `trr_backend/migrations/add_show_icons_table.sql`

**Step 1: Write the migration SQL**

```sql
-- Create show_icons table to track uploaded icon files per show
CREATE TABLE IF NOT EXISTS public.show_icons (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    show_key TEXT NOT NULL,
    filename TEXT NOT NULL,
    s3_key TEXT NOT NULL,
    hosted_url TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'image/png',
    file_bytes INTEGER,
    sha256 TEXT,
    width INTEGER,
    height INTEGER,
    uploaded_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE(show_key, s3_key)
);

-- Index for listing icons by show
CREATE INDEX IF NOT EXISTS idx_show_icons_show_key ON public.show_icons(show_key);

-- RLS: allow authenticated reads, admin writes handled at API level
ALTER TABLE public.show_icons ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read on show_icons"
    ON public.show_icons FOR SELECT
    USING (true);

CREATE POLICY "Allow service role all on show_icons"
    ON public.show_icons FOR ALL
    USING (auth.role() = 'service_role');
```

**Step 2: Apply the migration**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && psql "$SUPABASE_DB_URL" -f trr_backend/migrations/add_show_icons_table.sql`
Expected: `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE`, `CREATE POLICY` (x2)

If `psql` is not available or the DB URL isn't set locally, run via Supabase dashboard SQL editor instead.

**Step 3: Commit**

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git add trr_backend/migrations/add_show_icons_table.sql
git commit -m "feat: add show_icons table migration"
```

---

### Task 3: Create Backend Icon Upload/List/Delete Router

**Files:**
- Create: `api/routers/admin_show_icons.py`
- Modify: `api/main.py` (register router)
- Test: `tests/api/routers/test_admin_show_icons.py`

**Step 1: Write the failing test**

```python
# tests/api/routers/test_admin_show_icons.py
"""Tests for admin show icons API router."""
import io
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def mock_admin_user():
    """Mock the AdminUser dependency."""
    with patch("api.routers.admin_show_icons.AdminUser") as mock:
        mock.return_value = MagicMock(uid="test-admin")
        yield mock


def test_upload_icon_returns_hosted_url(mock_admin_user):
    """Upload icon should return hosted URL and icon metadata."""
    from api.main import app
    client = TestClient(app)

    # Create a small PNG file (1x1 pixel)
    png_bytes = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
        b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00"
        b"\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00"
        b"\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
    )

    with patch("api.routers.admin_show_icons.get_s3_client") as mock_s3, \
         patch("api.routers.admin_show_icons.get_s3_bucket", return_value="trr-backend"), \
         patch("api.routers.admin_show_icons.get_cdn_base_url", return_value="https://cdn.example.com"), \
         patch("api.routers.admin_show_icons._insert_icon_record") as mock_insert:
        mock_s3.return_value = MagicMock()
        mock_insert.return_value = {"id": "test-id", "hosted_url": "https://cdn.example.com/icons/rhobh/logo.png"}

        response = client.post(
            "/api/v1/admin/shows/rhobh/icons",
            files={"file": ("logo.png", io.BytesIO(png_bytes), "image/png")},
        )

    assert response.status_code == 200
    data = response.json()
    assert "hosted_url" in data
    assert "icons/rhobh/" in data["hosted_url"]


def test_list_icons_returns_array(mock_admin_user):
    """List icons should return an array of icon objects."""
    from api.main import app
    client = TestClient(app)

    with patch("api.routers.admin_show_icons._list_icon_records") as mock_list:
        mock_list.return_value = []
        response = client.get("/api/v1/admin/shows/rhobh/icons")

    assert response.status_code == 200
    assert response.json() == {"icons": []}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m pytest tests/api/routers/test_admin_show_icons.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'api.routers.admin_show_icons'`

**Step 3: Write the router implementation**

```python
# api/routers/admin_show_icons.py
"""Admin endpoints for managing show icon uploads."""

from __future__ import annotations

import hashlib
import io
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, File, HTTPException, UploadFile
from PIL import Image

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.s3_mirror import (
    build_icon_s3_key,
    get_cdn_base_url,
    get_icon_s3_prefix,
    get_s3_bucket,
    get_s3_client,
    list_s3_objects_under_prefix,
    upload_bytes_to_s3,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin-show-icons"])

MAX_ICON_BYTES = 5 * 1024 * 1024  # 5 MB
ALLOWED_ICON_TYPES = frozenset({
    "image/png",
    "image/jpeg",
    "image/webp",
    "image/svg+xml",
    "image/x-icon",
    "image/vnd.microsoft.icon",
})


def _extract_dimensions(data: bytes, content_type: str) -> tuple[int | None, int | None]:
    """Extract width/height from image bytes. Returns (None, None) for SVG/ICO."""
    if content_type in ("image/svg+xml", "image/x-icon", "image/vnd.microsoft.icon"):
        return None, None
    try:
        img = Image.open(io.BytesIO(data))
        return img.width, img.height
    except Exception:
        return None, None


def _insert_icon_record(
    db: Any,
    *,
    show_key: str,
    filename: str,
    s3_key: str,
    hosted_url: str,
    content_type: str,
    file_bytes: int,
    sha256: str,
    width: int | None,
    height: int | None,
    uploaded_by: str | None,
) -> dict[str, Any]:
    """Insert icon record into show_icons table."""
    row_id = str(uuid4())
    payload = {
        "id": row_id,
        "show_key": show_key,
        "filename": filename,
        "s3_key": s3_key,
        "hosted_url": hosted_url,
        "content_type": content_type,
        "file_bytes": file_bytes,
        "sha256": sha256,
        "width": width,
        "height": height,
        "uploaded_by": uploaded_by,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    response = db.table("show_icons").insert(payload).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to insert icon record: {response.error}")
    return response.data[0] if response.data else payload


def _list_icon_records(db: Any, show_key: str) -> list[dict[str, Any]]:
    """List all icon records for a show."""
    response = (
        db.table("show_icons")
        .select("*")
        .eq("show_key", show_key)
        .order("created_at", desc=True)
        .execute()
    )
    return response.data or []


def _delete_icon_record(db: Any, icon_id: str) -> dict[str, Any] | None:
    """Delete an icon record and return it."""
    response = (
        db.table("show_icons")
        .delete()
        .eq("id", icon_id)
        .execute()
    )
    return response.data[0] if response.data else None


@router.post("/admin/shows/{show_key}/icons")
async def upload_show_icon(
    show_key: str,
    file: UploadFile = File(...),
    _: AdminUser = None,
    db: Any = SupabaseAdminClient,
):
    """
    Upload an icon file for a show.

    Stores at s3://trr-backend/icons/{show_key}/{sanitized_filename}
    Returns the CDN URL and icon metadata.
    """
    # Validate content type
    ct = (file.content_type or "").split(";", 1)[0].strip().lower()
    if ct not in ALLOWED_ICON_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid content type '{ct}'. Allowed: {', '.join(sorted(ALLOWED_ICON_TYPES))}",
        )

    # Read and validate size
    data = await file.read()
    if len(data) > MAX_ICON_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"File too large ({len(data)} bytes). Maximum: {MAX_ICON_BYTES} bytes",
        )
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")

    # Compute SHA256
    sha256 = hashlib.sha256(data).hexdigest()

    # Extract dimensions
    width, height = _extract_dimensions(data, ct)

    # Build S3 key and upload
    original_filename = file.filename or "icon"
    s3_key = build_icon_s3_key(show_key=show_key, filename=original_filename)
    bucket = get_s3_bucket()
    s3 = get_s3_client()

    upload_bytes_to_s3(
        s3,
        bucket=bucket,
        key=s3_key,
        data=data,
        content_type=ct,
    )

    hosted_url = f"{get_cdn_base_url()}/{s3_key}"

    # Insert DB record
    record = _insert_icon_record(
        db,
        show_key=show_key,
        filename=original_filename,
        s3_key=s3_key,
        hosted_url=hosted_url,
        content_type=ct,
        file_bytes=len(data),
        sha256=sha256,
        width=width,
        height=height,
        uploaded_by=None,  # TODO: extract from AdminUser when available
    )

    logger.info("Uploaded icon for show=%s key=%s url=%s", show_key, s3_key, hosted_url)

    return {
        "id": record.get("id"),
        "show_key": show_key,
        "filename": original_filename,
        "s3_key": s3_key,
        "hosted_url": hosted_url,
        "content_type": ct,
        "file_bytes": len(data),
        "width": width,
        "height": height,
    }


@router.get("/admin/shows/{show_key}/icons")
async def list_show_icons(
    show_key: str,
    _: AdminUser = None,
    db: Any = SupabaseAdminClient,
):
    """List all icons for a show."""
    icons = _list_icon_records(db, show_key)
    return {"icons": icons}


@router.delete("/admin/shows/{show_key}/icons/{icon_id}")
async def delete_show_icon(
    show_key: str,
    icon_id: str,
    _: AdminUser = None,
    db: Any = SupabaseAdminClient,
):
    """
    Delete an icon: remove from S3 and database.
    """
    # Fetch the record first to get the S3 key
    response = (
        db.table("show_icons")
        .select("*")
        .eq("id", icon_id)
        .eq("show_key", show_key)
        .single()
        .execute()
    )
    record = response.data if response.data else None
    if not record:
        raise HTTPException(status_code=404, detail="Icon not found")

    # Delete from S3
    s3_key = record["s3_key"]
    bucket = get_s3_bucket()
    s3 = get_s3_client()
    try:
        s3.delete_object(Bucket=bucket, Key=s3_key)
    except Exception as exc:
        logger.warning("Failed to delete S3 object %s: %s", s3_key, exc)

    # Delete DB record
    _delete_icon_record(db, icon_id)

    logger.info("Deleted icon id=%s show=%s key=%s", icon_id, show_key, s3_key)
    return {"deleted": True, "id": icon_id}
```

**Step 4: Register the router in `api/main.py`**

Add to imports (around line 72):
```python
    admin_show_icons,
```

Add router registration (around line 113):
```python
app.include_router(admin_show_icons.router, prefix="/api/v1")
```

**Step 5: Run tests to verify they pass**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m pytest tests/api/routers/test_admin_show_icons.py -v`
Expected: PASS

**Step 6: Verify type checking**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m py_compile api/routers/admin_show_icons.py`
Expected: No output (success)

**Step 7: Commit**

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git add api/routers/admin_show_icons.py api/main.py tests/api/routers/test_admin_show_icons.py
git commit -m "feat: add admin show icons upload/list/delete router"
```

---

### Task 4: Create Next.js API Proxy Routes for Icons

**Files:**
- Create: `apps/web/src/app/api/admin/shows/[showKey]/icons/route.ts`
- Create: `apps/web/src/app/api/admin/shows/[showKey]/icons/[iconId]/route.ts`

**Step 1: Create the upload + list proxy route**

```typescript
// apps/web/src/app/api/admin/shows/[showKey]/icons/route.ts
import { NextRequest, NextResponse } from "next/server";
import { requireAdmin } from "@/lib/server/auth";

const TRR_API_URL = process.env.TRR_API_URL || "http://127.0.0.1:8000";

/**
 * POST /api/admin/shows/[showKey]/icons
 * Proxy multipart upload to backend.
 */
export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ showKey: string }> }
) {
  const admin = await requireAdmin(request);
  if (!admin) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { showKey } = await params;

  // Forward the multipart form data as-is
  const formData = await request.formData();
  const backendUrl = `${TRR_API_URL}/api/v1/admin/shows/${showKey}/icons`;

  const resp = await fetch(backendUrl, {
    method: "POST",
    body: formData,
    headers: {
      Authorization: request.headers.get("Authorization") || "",
    },
  });

  const data = await resp.json().catch(() => ({}));
  return NextResponse.json(data, { status: resp.status });
}

/**
 * GET /api/admin/shows/[showKey]/icons
 * List all icons for a show.
 */
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ showKey: string }> }
) {
  const admin = await requireAdmin(request);
  if (!admin) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { showKey } = await params;
  const backendUrl = `${TRR_API_URL}/api/v1/admin/shows/${showKey}/icons`;

  const resp = await fetch(backendUrl, {
    headers: {
      Authorization: request.headers.get("Authorization") || "",
    },
  });

  const data = await resp.json().catch(() => ({}));
  return NextResponse.json(data, { status: resp.status });
}
```

**Step 2: Create the delete proxy route**

```typescript
// apps/web/src/app/api/admin/shows/[showKey]/icons/[iconId]/route.ts
import { NextRequest, NextResponse } from "next/server";
import { requireAdmin } from "@/lib/server/auth";

const TRR_API_URL = process.env.TRR_API_URL || "http://127.0.0.1:8000";

/**
 * DELETE /api/admin/shows/[showKey]/icons/[iconId]
 * Delete an icon.
 */
export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ showKey: string; iconId: string }> }
) {
  const admin = await requireAdmin(request);
  if (!admin) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { showKey, iconId } = await params;
  const backendUrl = `${TRR_API_URL}/api/v1/admin/shows/${showKey}/icons/${iconId}`;

  const resp = await fetch(backendUrl, {
    method: "DELETE",
    headers: {
      Authorization: request.headers.get("Authorization") || "",
    },
  });

  const data = await resp.json().catch(() => ({}));
  return NextResponse.json(data, { status: resp.status });
}
```

**Step 3: Verify type checking**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-APP && npx tsc --noEmit`
Expected: No errors

**Step 4: Commit**

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-APP
git add apps/web/src/app/api/admin/shows/\[showKey\]/icons/
git commit -m "feat: add Next.js proxy routes for show icon upload/list/delete"
```

---

### Task 5: Create `IconUploader` React Component

**Files:**
- Create: `apps/web/src/components/admin/IconUploader.tsx`

**Step 1: Write the component**

This is a reusable drag-and-drop icon upload component with preview grid.

```tsx
// apps/web/src/components/admin/IconUploader.tsx
"use client";

import { useCallback, useEffect, useState } from "react";
import { useAuth } from "@/lib/client/auth";

interface IconRecord {
  id: string;
  show_key: string;
  filename: string;
  s3_key: string;
  hosted_url: string;
  content_type: string;
  file_bytes: number;
  width: number | null;
  height: number | null;
  created_at?: string;
}

interface IconUploaderProps {
  showKey: string;
  /** Called when an icon is selected (clicked) from the library */
  onSelect?: (icon: IconRecord) => void;
  /** Compact mode: single row, no grid */
  compact?: boolean;
}

export function IconUploader({ showKey, onSelect, compact = false }: IconUploaderProps) {
  const { getAuthHeaders } = useAuth();
  const [icons, setIcons] = useState<IconRecord[]>([]);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState(false);

  const fetchIcons = useCallback(async () => {
    try {
      const headers = await getAuthHeaders();
      const resp = await fetch(`/api/admin/shows/${showKey}/icons`, { headers });
      if (!resp.ok) return;
      const data = await resp.json();
      setIcons(data.icons || []);
    } catch {
      // Silent fail on list
    }
  }, [showKey, getAuthHeaders]);

  useEffect(() => {
    fetchIcons();
  }, [fetchIcons]);

  const uploadFile = async (file: File) => {
    setUploading(true);
    setError(null);
    try {
      const headers = await getAuthHeaders();
      const formData = new FormData();
      formData.append("file", file);

      const resp = await fetch(`/api/admin/shows/${showKey}/icons`, {
        method: "POST",
        headers: { Authorization: headers.Authorization },
        body: formData,
      });

      if (!resp.ok) {
        const data = await resp.json().catch(() => ({}));
        throw new Error(data.detail || `Upload failed (${resp.status})`);
      }

      await fetchIcons();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed");
    } finally {
      setUploading(false);
    }
  };

  const deleteIcon = async (iconId: string) => {
    try {
      const headers = await getAuthHeaders();
      await fetch(`/api/admin/shows/${showKey}/icons/${iconId}`, {
        method: "DELETE",
        headers,
      });
      await fetchIcons();
    } catch {
      setError("Failed to delete icon");
    }
  };

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(false);
      const file = e.dataTransfer.files[0];
      if (file) uploadFile(file);
    },
    [showKey]
  );

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) uploadFile(file);
    e.target.value = "";
  };

  return (
    <div className="space-y-3">
      {/* Drop zone */}
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
        className={`relative flex items-center justify-center rounded-lg border-2 border-dashed p-4 transition-colors ${
          dragOver
            ? "border-blue-400 bg-blue-50"
            : "border-zinc-300 bg-zinc-50 hover:border-zinc-400"
        } ${compact ? "h-20" : "h-32"}`}
      >
        {uploading ? (
          <p className="text-sm text-zinc-500">Uploading...</p>
        ) : (
          <div className="text-center">
            <p className="text-sm text-zinc-500">
              Drop icon here or{" "}
              <label className="cursor-pointer text-blue-600 hover:underline">
                browse
                <input
                  type="file"
                  className="hidden"
                  accept="image/png,image/jpeg,image/webp,image/svg+xml,.ico"
                  onChange={handleFileInput}
                />
              </label>
            </p>
            <p className="mt-1 text-xs text-zinc-400">PNG, JPG, WebP, SVG, ICO (max 5MB)</p>
          </div>
        )}
      </div>

      {error && <p className="text-sm text-red-600">{error}</p>}

      {/* Icon grid */}
      {icons.length > 0 && (
        <div className={compact ? "flex gap-2 overflow-x-auto" : "grid grid-cols-4 gap-3 sm:grid-cols-6"}>
          {icons.map((icon) => (
            <div
              key={icon.id}
              className="group relative flex flex-col items-center rounded-lg border border-zinc-200 bg-white p-2"
            >
              <button
                type="button"
                onClick={() => onSelect?.(icon)}
                className="flex h-12 w-12 items-center justify-center"
                title={`Select ${icon.filename}`}
              >
                <img
                  src={icon.hosted_url}
                  alt={icon.filename}
                  className="max-h-12 max-w-12 object-contain"
                />
              </button>
              {!compact && (
                <p className="mt-1 max-w-full truncate text-xs text-zinc-500" title={icon.filename}>
                  {icon.filename}
                </p>
              )}
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  deleteIcon(icon.id);
                }}
                className="absolute -right-1 -top-1 hidden rounded-full bg-red-500 p-0.5 text-white group-hover:block"
                title="Delete icon"
              >
                <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
```

**Step 2: Verify type checking**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-APP && npx tsc --noEmit`
Expected: No errors

**Step 3: Commit**

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-APP
git add apps/web/src/components/admin/IconUploader.tsx
git commit -m "feat: add IconUploader drag-and-drop component"
```

---

### Task 6: Integrate `IconUploader` into ShowBrandEditor

**Files:**
- Modify: `apps/web/src/components/admin/ShowBrandEditor.tsx` (lines ~858-891)

**Step 1: Add import at top of file**

Add near other imports:
```tsx
import { IconUploader } from "./IconUploader";
```

**Step 2: Replace the icon text input with IconUploader + text input combo**

Replace the icon `<label>` block (lines 861-870) with:

```tsx
<div className="block">
  <span className="mb-1 block text-sm font-semibold text-zinc-700">Icon</span>
  <div className="space-y-2">
    <input
      type="text"
      value={iconUrl}
      onChange={(e) => setIconUrl(e.target.value)}
      className="w-full rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm focus:border-zinc-400 focus:outline-none"
      placeholder="https://... or upload below"
    />
    {iconUrl && (
      <div className="flex items-center gap-2">
        <img src={iconUrl} alt="Current icon" className="h-8 w-8 object-contain" />
        <span className="text-xs text-zinc-500">Current icon</span>
      </div>
    )}
    {showRecord && (
      <IconUploader
        showKey={showRecord.key}
        compact
        onSelect={(icon) => setIconUrl(icon.hosted_url)}
      />
    )}
  </div>
</div>
```

**Step 3: Verify type checking**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-APP && npx tsc --noEmit`
Expected: No errors

**Step 4: Commit**

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-APP
git add apps/web/src/components/admin/ShowBrandEditor.tsx
git commit -m "feat: integrate IconUploader into ShowBrandEditor icon field"
```

---

### Task 7: Create Icon Library Admin Page

**Files:**
- Create: `apps/web/src/app/(admin)/admin/shows/[showKey]/icons/page.tsx`

**Step 1: Write the page component**

```tsx
// apps/web/src/app/(admin)/admin/shows/[showKey]/icons/page.tsx
import { redirect } from "next/navigation";
import { requireAdmin } from "@/lib/server/auth";
import { getShowByKey } from "@/lib/server/shows/shows-repository";
import { IconLibraryClient } from "./icon-library-client";

interface PageProps {
  params: Promise<{ showKey: string }>;
}

export default async function ShowIconsPage({ params }: PageProps) {
  const admin = await requireAdmin();
  if (!admin) redirect("/login");

  const { showKey } = await params;
  const show = await getShowByKey(showKey);
  if (!show) redirect("/admin/shows");

  return (
    <div className="mx-auto max-w-4xl px-4 py-8">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-zinc-900">
          {show.title} - Icon Library
        </h1>
        <p className="mt-1 text-sm text-zinc-500">
          Upload and manage icons for this show. Click an icon to copy its URL.
        </p>
      </div>

      <IconLibraryClient showKey={showKey} />
    </div>
  );
}
```

**Step 2: Create the client component**

```tsx
// apps/web/src/app/(admin)/admin/shows/[showKey]/icons/icon-library-client.tsx
"use client";

import { IconUploader } from "@/components/admin/IconUploader";
import { useState } from "react";

interface IconLibraryClientProps {
  showKey: string;
}

export function IconLibraryClient({ showKey }: IconLibraryClientProps) {
  const [copiedUrl, setCopiedUrl] = useState<string | null>(null);

  const handleSelect = (icon: { hosted_url: string; filename: string }) => {
    navigator.clipboard.writeText(icon.hosted_url).then(() => {
      setCopiedUrl(icon.hosted_url);
      setTimeout(() => setCopiedUrl(null), 2000);
    });
  };

  return (
    <div>
      {copiedUrl && (
        <div className="mb-4 rounded-lg border border-green-200 bg-green-50 px-4 py-2 text-sm text-green-700">
          Copied URL to clipboard!
        </div>
      )}
      <IconUploader showKey={showKey} onSelect={handleSelect} />
    </div>
  );
}
```

**Step 3: Verify type checking**

Run: `cd /Users/thomashulihan/Projects/TRR/TRR-APP && npx tsc --noEmit`
Expected: No errors

**Step 4: Commit**

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-APP
git add "apps/web/src/app/(admin)/admin/shows/[showKey]/icons/"
git commit -m "feat: add show icon library admin page"
```

---

### Task 8: Manual E2E Smoke Test

**Step 1: Start dev servers**

Run: `cd /Users/thomashulihan/Projects/TRR && make dev`

**Step 2: Apply database migration**

Apply the SQL from Task 2 to the local/dev Supabase instance.

**Step 3: Test icon upload via backend directly**

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/admin/shows/rhobh/icons" \
  -H "Authorization: Bearer <admin-token>" \
  -F "file=@/path/to/test-icon.png"
```

Expected: JSON response with `hosted_url` like `https://d1fmdyqfafwim3.cloudfront.net/icons/rhobh/test-icon.png`

**Step 4: Test icon listing**

```bash
curl "http://127.0.0.1:8000/api/v1/admin/shows/rhobh/icons" \
  -H "Authorization: Bearer <admin-token>"
```

Expected: JSON with `{"icons": [{ ... }]}`

**Step 5: Test via frontend**

1. Navigate to `http://127.0.0.1:3000/admin/shows/rhobh/icons`
2. Verify icon library page loads
3. Drag and drop a PNG icon
4. Verify it appears in the grid
5. Click the icon - verify URL is copied

**Step 6: Test ShowBrandEditor integration**

1. Navigate to the show brand editor for any show
2. Scroll to "Asset URLs" section
3. Verify upload zone appears below icon text input
4. Upload an icon
5. Click the icon in the grid
6. Verify the icon URL populates the text input
7. Save the show
8. Reload and verify the icon URL persists

**Step 7: Verify S3 storage**

```bash
aws s3 ls s3://trr-backend/icons/ --recursive --profile trr
```

Expected: Files listed under `icons/{show_key}/` prefix

**Step 8: Commit any final adjustments**

```bash
git add -A && git commit -m "fix: final adjustments from E2E smoke test"
```
