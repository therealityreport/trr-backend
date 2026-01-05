"""
Domain models shared across scripts and services.
"""

from trr_backend.models.cast_photos import CastPhotoUpsert
from trr_backend.models.shows import ShowRecord, ShowUpsert

__all__ = [
    "ShowRecord",
    "ShowUpsert",
    "CastPhotoUpsert",
]
