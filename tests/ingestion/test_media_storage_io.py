"""Owned real subprocesses with synthetic SDKs; no network or credentials."""

import hashlib
import json
import os
import signal
import subprocess
import sys
import textwrap
import time

import psycopg2
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.media import MediaIncomplete, MediaOwnershipCheck, MediaStorage
from trr_backend.ingestion.media_storage_io import BoundedS3Client

BUCKET = "private-process-fixture"
SECRET = "must-not-log-synthetic-private-key"
STUB = """
import hashlib,io,json,os,time
from pathlib import Path
from botocore.exceptions import ClientError
from trr_backend.ingestion.media_storage_io import main
class Body:
 def read(self,size):
  time.sleep(.02)
  return b'x'
 def close(self):pass
class SDK:
 def __init__(self,spec):
  self.root=Path(os.environ['TRR_TEST_STORAGE_ROOT'])
  self.root.joinpath('pid').write_text(str(os.getpid()))
 def head_object(self,**kwargs):
  if kwargs['Key']=='blocked':
   while True:time.sleep(.02)
  if kwargs['Key']=='safe':
   return {'ContentLength':7,'Metadata':{'sha256':'a'*64,'private':'discard'}}
  if kwargs['Key']=='denied':
   response={'Error':{'Code':'AccessDenied','Message':'private-details-excluded'},'ResponseMetadata':{'HTTPStatusCode':403}}
   raise ClientError(response,'HeadObject')
  path=self.root/'object'
  if not path.exists():
   raise ClientError({'Error':{'Code':'404'},'ResponseMetadata':{'HTTPStatusCode':404}},'HeadObject')
  data=path.read_bytes()
  return {'ContentLength':len(data),'Metadata':{'sha256':hashlib.sha256(data).hexdigest()}}
 def put_object(self,**kwargs):
  if kwargs['Key']=='conflict':
   raise ClientError({'Error':{'Code':'PreconditionFailed'},'ResponseMetadata':{'HTTPStatusCode':412}},'PutObject')
  self.root.joinpath('object').write_bytes(kwargs['Body'])
  self.root.joinpath('conditional').write_text(kwargs['IfNoneMatch'])
  if kwargs['Key']=='slow-put':
   while True:time.sleep(.02)
  return {}
 def get_object(self,**kwargs):
  if kwargs['Key']=='progressing':return {'Body':Body()}
  return {'Body':io.BytesIO(b'persisted-bytes')}
 def close(self):pass
raise SystemExit(main(client_factory=SDK))
"""


def proxy(tmp_path, monkeypatch, *, entrypoint=None):
    monkeypatch.setenv("TRR_TEST_STORAGE_ROOT", str(tmp_path))
    return BoundedS3Client(
        bucket=BUCKET,
        profile_name=None,
        client_options={
            "region_name": "auto",
            "endpoint_url": "https://storage.invalid",
            "aws_access_key_id": "synthetic-access",
            "aws_secret_access_key": SECRET,
            "config": Config(
                connect_timeout=0.1,
                read_timeout=0.1,
                retries={"total_max_attempts": 1},
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        },
        _entrypoint=entrypoint or (sys.executable, "-c", textwrap.dedent(STUB)),
    )


def ownership(seconds=3):
    return MediaOwnershipCheck(lambda: None, deadline=time.monotonic() + seconds)


@pytest.fixture
def children(monkeypatch):
    from trr_backend.ingestion import media_storage_io

    started = []
    original = subprocess.Popen

    def spawn(*args, **kwargs):
        assert kwargs["start_new_session"] is False
        assert kwargs["stderr"] == subprocess.DEVNULL
        assert SECRET not in repr(args)
        process = original(*args, **kwargs)
        assert os.getpgid(process.pid) == os.getpgrp()
        started.append(process)
        return process

    monkeypatch.setattr(media_storage_io.subprocess, "Popen", spawn)
    yield started
    for process in started:
        assert process.poll() is not None
        with pytest.raises(ChildProcessError):
            os.waitpid(process.pid, os.WNOHANG)


def test_head_projection_and_private_pipe_do_not_expose_configuration(tmp_path, monkeypatch, children):
    client = proxy(tmp_path, monkeypatch)
    assert client.call("head_object", check=ownership(), Bucket=BUCKET, Key="safe") == {
        "ContentLength": 7,
        "Metadata": {"sha256": "a" * 64},
    }
    assert client._active is None
    with pytest.raises(ClientError) as failure:
        client.call("head_object", check=ownership(), Bucket=BUCKET, Key="denied")
    assert failure.value.response == {"Error": {"Code": "AccessDenied"}, "ResponseMetadata": {"HTTPStatusCode": 403}}
    assert "private-details" not in str(failure.value)
    assert len(children) == 2


def test_child_put_and_digest_return_only_verified_bounded_results(tmp_path, monkeypatch, children):
    client = proxy(tmp_path, monkeypatch)
    body = b"synthetic object body"
    assert (
        client.call(
            "put_object",
            check=ownership(),
            Bucket=BUCKET,
            Key="role",
            Body=body,
            IfNoneMatch="*",
            ContentType="image/png",
            Metadata={"sha256": "wrong"},
        )
        == {}
    )
    assert (tmp_path / "object").read_bytes() == body
    assert (tmp_path / "conditional").read_text() == "*"
    digest = client.call("digest_object", check=ownership(), Bucket=BUCKET, Key="role")
    assert digest == {"bytes": 15, "sha256": hashlib.sha256(b"persisted-bytes").hexdigest()}
    assert len(children) == 2 and client._active is None


@pytest.mark.parametrize("operation,key", [("digest_object", "progressing"), ("put_object", "slow-put")])
def test_progress_below_inactivity_timeout_cannot_extend_absolute_cap(tmp_path, monkeypatch, children, operation, key):
    client = proxy(tmp_path, monkeypatch)
    service = MediaStorage(client=client, bucket=BUCKET, build_hosted_url=lambda key: "https://media.invalid/" + key)
    kwargs = {"Bucket": BUCKET, "Key": key}
    if operation == "put_object":
        kwargs.update(Body=b"finished-before-response", IfNoneMatch="*", ContentType="image/png")
    started = time.monotonic()
    with pytest.raises(MediaIncomplete, match="target_deadline"):
        service._storage_call(operation, check=ownership(0.8), **kwargs)  # pyright: ignore[reportArgumentType]
    assert (tmp_path / "pid").exists(), "SDK started before its wall-clock cap"
    assert time.monotonic() - started < 2.5
    assert len(children) == 1 and children[0].poll() is not None and client._active is None
    if operation == "put_object":
        assert (tmp_path / "object").read_bytes() == b"finished-before-response"


@pytest.mark.parametrize(
    "failure", [LostOwnership("fixture owner cancelled"), psycopg2.OperationalError("fixture database failure")]
)
def test_original_owner_failure_kills_blocked_sdk_and_is_not_reclassified(tmp_path, monkeypatch, children, failure):
    client = proxy(tmp_path, monkeypatch)
    deadline = time.monotonic() + 5

    class Check:
        def __call__(self):
            if (tmp_path / "pid").exists():
                raise failure

        def remaining(self):
            return deadline - time.monotonic()

    with pytest.raises(type(failure)) as caught:
        client.call("head_object", check=Check(), Bucket=BUCKET, Key="blocked")
    assert caught.value is failure
    assert len(children) == 1 and client._active is None


def test_completed_put_with_lost_response_is_retained_and_retry_is_idempotent(tmp_path, monkeypatch, children):
    client = proxy(tmp_path, monkeypatch)
    service = MediaStorage(client=client, bucket=BUCKET, build_hosted_url=lambda key: "https://media.invalid/" + key)
    body = b"preserved immutable bytes"
    with pytest.raises(MediaIncomplete, match="target_deadline"):
        service.put("slow-put", body, "image/png", check=ownership(1), request=lambda: None)
    assert (tmp_path / "object").read_bytes() == body
    before = (tmp_path / "object").stat().st_mtime_ns
    service.put("slow-put", body, "image/png", check=ownership(3), request=lambda: None)
    assert (tmp_path / "object").stat().st_mtime_ns == before
    assert len(children) == 3 and client._active is None


def is_live(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # An adopted zombie has no SDK execution or open transport; init owns reaping.
    state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return bool(state) and not state.startswith("Z")


@pytest.mark.parametrize("stop", ["parent-only", "outer-group"])
def test_abrupt_activity_exit_does_not_orphan_sdk_transport(tmp_path, monkeypatch, stop):
    client = proxy(tmp_path, monkeypatch)
    # The wrapper receives all constructor configuration through stdin too.
    code = """
import json,sys,time
from botocore.config import Config
from trr_backend.ingestion.media import MediaOwnershipCheck
from trr_backend.ingestion.media_storage_io import BoundedS3Client
spec=json.loads(sys.stdin.read())
options=spec['client_options'];options['config']=Config(**spec['config'])
client=BoundedS3Client(bucket=spec['bucket'],profile_name=spec['profile_name'],client_options=options,_entrypoint=spec['entrypoint'])
client.call('head_object',check=MediaOwnershipCheck(lambda:None,deadline=time.monotonic()+20),Bucket=spec['bucket'],Key='blocked')
"""
    envelope = {**client._spec, "entrypoint": client._entrypoint}
    wrapper = subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(code)],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    assert wrapper.stdin is not None
    wrapper.stdin.write(json.dumps(envelope).encode())
    wrapper.stdin.close()
    try:
        limit = time.monotonic() + 5
        while not (tmp_path / "pid").exists() and time.monotonic() < limit:
            time.sleep(0.02)
        sdk_pid = int((tmp_path / "pid").read_text())
        assert os.getpgid(sdk_pid) == wrapper.pid
        if stop == "parent-only":
            wrapper.kill()
        else:
            os.killpg(wrapper.pid, signal.SIGTERM)
        wrapper.wait(timeout=2)
        limit = time.monotonic() + 2
        while is_live(sdk_pid) and time.monotonic() < limit:
            time.sleep(0.02)
        assert not is_live(sdk_pid)
    finally:
        if wrapper.poll() is None:
            os.killpg(wrapper.pid, signal.SIGKILL)
            wrapper.wait(timeout=2)


def test_production_factory_builds_owner_bounded_proxy_without_eager_sdk_or_avatar_changes(monkeypatch):
    from types import SimpleNamespace

    import boto3

    from trr_backend.ingestion import composition, media_refresh
    from trr_backend.media import s3_mirror

    monkeypatch.setattr(boto3, "Session", lambda **kwargs: pytest.fail("SDK construction escaped bounded child"))
    configuration = SimpleNamespace(
        profile_name="fixture-profile",
        region="auto",
        endpoint_url="https://storage.invalid",
        access_key_id="synthetic-access",
        secret_access_key=SECRET,
        session_token=None,
        bucket=BUCKET,
        prefix="retained-prefix",
    )
    monkeypatch.setattr(s3_mirror, "get_hosted_media_storage_config", lambda: configuration)
    monkeypatch.setattr(composition, "_source_profile", lambda context: SimpleNamespace(access_mode="public"))
    store = object()
    monkeypatch.setattr(composition, "_worker_store", lambda context: store)
    monkeypatch.setattr(composition, "_network_lane", lambda context: "local")
    monkeypatch.setattr(media_refresh, "MediaRefresh", lambda **kwargs: SimpleNamespace(**kwargs))
    service = composition._media_storage({"target": {"owner_expires_at": "unchanged"}}, {"account": "bravotv"})
    assert isinstance(service.client, BoundedS3Client)
    assert service.client._spec["profile_name"] == configuration.profile_name
    assert service.client._spec["client_options"] == {
        "region_name": "auto",
        "endpoint_url": "https://storage.invalid",
        "aws_access_key_id": "synthetic-access",
        "aws_secret_access_key": SECRET,
    }
    assert service.client._spec["config"] == {
        "connect_timeout": 10,
        "read_timeout": 60,
        "retries": {"total_max_attempts": 1},
        "signature_version": "s3v4",
        "s3": {"addressing_style": "path"},
    }
    from trr_backend.ingestion.media_storage_io import MAX_BODY_BYTES

    assert service.max_bytes == MAX_BODY_BYTES == 50 * 1024 * 1024
    assert service.refresh is not None
    assert service.prefix == "retained-prefix" and service.refresh.store is store  # pyright: ignore[reportFunctionMemberAccess]
    assert callable(service.refresh.public_avatar_source_factory) and service.refresh.public_only is True  # pyright: ignore[reportFunctionMemberAccess]


def test_child_reconstructs_the_exact_session_and_explicit_sdk_configuration(tmp_path, monkeypatch):
    import boto3

    from trr_backend.ingestion.media_storage_io import _build_client

    client = proxy(tmp_path, monkeypatch)
    captured = {}
    result = object()

    class Session:
        def __init__(self, **kwargs):
            captured["session"] = kwargs

        def client(self, service, **kwargs):
            captured["service"], captured["options"] = service, kwargs
            return result

    monkeypatch.setattr(boto3, "Session", Session)
    assert _build_client(client._spec) is result
    assert captured["service"] == "s3" and captured["session"] == {}
    options = captured["options"]
    config = options.pop("config")
    assert options == client._spec["client_options"]
    assert config.connect_timeout == config.read_timeout == 0.1
    assert config.retries == {"total_max_attempts": 1} and config.s3 == {"addressing_style": "path"}


def test_child_conditional_conflict_keeps_http_412_mapping_and_no_object_write(tmp_path, monkeypatch, children):
    client = proxy(tmp_path, monkeypatch)
    with pytest.raises(ClientError) as caught:
        client.call(
            "put_object",
            check=ownership(),
            Bucket=BUCKET,
            Key="conflict",
            Body=b"immutable",
            ContentType="image/png",
            IfNoneMatch="*",
        )
    assert caught.value.response == {
        "Error": {"Code": "PreconditionFailed"},
        "ResponseMetadata": {"HTTPStatusCode": 412},
    }
    assert not (tmp_path / "object").exists() and len(children) == 1
