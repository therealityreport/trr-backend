"""One v2 profile-enabled public pipeline through real production composition."""

from tests.ingestion import test_profiles, test_public_pipeline

base_database = test_profiles.base_database
pipeline_database = test_profiles.pipeline_database
profiles_db = test_profiles.profiles_db
clear = test_profiles.clear


def test_public_v2_profiles_join_existing_durable_pipeline(profiles_db, monkeypatch):
    test_public_pipeline.test_public_composed_pipeline_commits_native_parent_reply_post_and_avatar_media(
        profiles_db, monkeypatch, profile_mode=True
    )
