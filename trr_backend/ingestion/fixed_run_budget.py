"""Durable operator bounds for an exact local fixed-post run, never source health."""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from trr_backend.ingestion.domain import fingerprint
from trr_backend.ingestion.identity_policy import IdentityDeferred


class FixedRunBudget:
    def __init__(self, directory, run_id, manifest_hash, limit, deadline, *, clock=time.time):
        self.run_id = str(UUID(str(run_id)))
        self.limit, self.deadline, self.clock = limit, deadline, clock
        self.binding = fingerprint(
            {"run": self.run_id, "manifest": manifest_hash, "limit": limit, "deadline": deadline}
        )
        self.monotonic_deadline = time.monotonic() + deadline - clock()
        root = Path(directory).expanduser()
        try:
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            if root.is_symlink() or root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077:
                raise ValueError("Private operator budget directory required")
            self.path = root / (self.run_id + ".sqlite3")
            fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            if self.path.stat().st_mode & 0o077:
                raise ValueError("Private operator budget file required")
            with closing(sqlite3.connect(self.path)) as db:
                db.execute("create table if not exists budget (id integer primary key, binding text, consumed integer)")
                db.execute("insert or ignore into budget values (1,?,0)", (self.binding,))
                db.commit()
                if db.execute("select binding from budget where id=1").fetchone()[0] != self.binding:
                    raise ValueError("Operator budget binding changed")
            self.check()
        except IdentityDeferred:
            raise
        except (OSError, ValueError, sqlite3.Error):
            raise IdentityDeferred("operator_budget_unavailable", None, blocked=True) from None

    @classmethod
    def from_context(cls, context):
        setup = context["target"].get("operator_selection") or {}
        manifest = setup.get("manifest") if isinstance(setup, dict) else None
        if not isinstance(setup, dict) or setup.get("kind") != "operator_selection_setup":
            return None
        try:
            from trr_backend.ingestion.fixed_validation import FixedManifest

            frozen = FixedManifest.from_payload(manifest)
            run, target = context["run"], context["target"]
            directory = os.getenv("TRR_INSTAGRAM_LOCAL_WORKER_BUDGET_DIR")
            if (
                setup.get("kind") != "operator_selection_setup"
                or frozen.digest != setup.get("manifest_hash")
                or run.get("execution_placement") != "local"
                or frozen.intent().payload() != run["intent"]
                or frozen.intent().digest != run["intent_hash"]
                or target["stage"] not in frozen.stages
                or target["target_key"] not in {post.shortcode for post in frozen.posts}
            ):
                raise ValueError("Frozen local budget binding required")
            if frozen.max_source_requests is None:
                return None
            if not directory:
                raise ValueError("Private operator budget directory required")
            created = run["created_at"]
            if isinstance(created, str):
                created = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if created.utcoffset() is None:
                raise ValueError("Timezone-aware run start required")
            return cls(
                directory,
                run["run_id"],
                frozen.digest,
                frozen.max_source_requests,
                created.timestamp() + frozen.max_run_seconds,
            )
        except IdentityDeferred:
            raise
        except (KeyError, TypeError, ValueError, AttributeError):
            raise IdentityDeferred("operator_budget_invalid", None, blocked=True) from None

    @property
    def expires_at(self):
        return datetime.fromtimestamp(self.deadline, UTC)

    def check(self):
        if self.clock() >= self.deadline:
            raise IdentityDeferred("operator_time_limit", None, blocked=True)

    def consume(self):
        try:
            with closing(sqlite3.connect(self.path, timeout=2)) as db:
                db.execute("begin immediate")
                binding, count = db.execute("select binding,consumed from budget where id=1").fetchone()
                if binding != self.binding:
                    raise ValueError("Operator budget binding changed")
                self.check()
                if count >= self.limit:
                    raise IdentityDeferred("operator_request_limit", None, blocked=True)
                db.execute("update budget set consumed=consumed+1 where id=1")
                db.commit()
        except IdentityDeferred:
            raise
        except (OSError, ValueError, TypeError, sqlite3.Error):
            raise IdentityDeferred("operator_budget_unavailable", None, blocked=True) from None

    def wrap(self, callback):
        def guarded(*args, **kwargs):
            self.consume()
            return callback(*args, **kwargs)

        guarded.__dict__.update(getattr(callback, "__dict__", {}))
        for name in ("in_transaction", "finalize_in_transaction"):
            hook = getattr(callback, name, None)
            if callable(hook):
                setattr(guarded, name, self.wrap(hook))
        deadlines = [self.monotonic_deadline - 20]
        if getattr(callback, "deadline", None) is not None:
            deadlines.append(callback.deadline)
        guarded.deadline = min(deadlines)
        return guarded

    def callbacks(self, callbacks):
        return {
            name: self.wrap(value) if name in {"on_request", "pre_source_request"} else value
            for name, value in callbacks.items()
        }
