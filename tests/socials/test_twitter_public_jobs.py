import copy
import json
from contextlib import contextmanager

import pytest

from trr_backend.socials.twitter import public_jobs

ACCEPTED = dict.fromkeys(
    ["rhoslc_search", "bravotv_profile", "nullable_schema", "known_value_merge", "consumer_nulls"], True
)


def test_claimed_job_retains_checkpoint_and_rejects_stale_owner(monkeypatch):
    config = {"public_x": {"provider": "fxtwitter", "query": "#RHOSLC", "max_pages": 1, "max_items": 2}}
    owned = True

    @contextmanager
    def scope(*a, **kw):
        yield owned

    monkeypatch.setattr(public_jobs, "_job_claim_write_scope", scope)
    monkeypatch.setattr(
        public_jobs.pg, "fetch_one", lambda *a, **kw: {"platform": "twitter", "config": copy.deepcopy(config)}
    )

    def execute(sql, args):
        config["public_x_checkpoint"] = json.loads(args[0])

    monkeypatch.setattr(public_jobs.pg, "execute", execute)

    def fetch(*a, **kw):
        return json.dumps(
            {
                "code": 200,
                "results": [{"id": "100", "text": "#RHOSLC", "author": {"screen_name": "BravoTV"}}],
                "cursor": {"bottom": "next"},
            }
        ).encode()

    r = public_jobs.run_claimed_public_collection(
        "id", worker_id="owner", claim_token="acquired", acceptance=ACCEPTED, fetcher=fetch
    )
    assert config["public_x_checkpoint"]["cursor"] == "next" and r["metadata"]["returned"] == 1
    owned = False
    with pytest.raises(public_jobs.PublicJobOwnershipLostError):
        public_jobs.run_claimed_public_collection(
            "id", worker_id="stale", claim_token="old", acceptance=ACCEPTED, fetcher=fetch
        )


def test_gate_precedes_database_access(monkeypatch):
    monkeypatch.setattr(public_jobs.pg, "fetch_one", lambda *a, **kw: pytest.fail("must not read DB"))
    with pytest.raises(ValueError, match="gates"):
        public_jobs.run_claimed_public_collection("id", worker_id="owner", claim_token="acquired", acceptance={})
