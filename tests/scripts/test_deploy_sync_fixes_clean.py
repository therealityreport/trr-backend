from __future__ import annotations

import json

from scripts.modal.deploy_sync_fixes_clean import load_sync_fix_config


def test_load_sync_fix_config_reads_paths_and_required_dirs(tmp_path) -> None:
    config_path = tmp_path / "sync_fix_deploy_paths.json"
    config_path.write_text(
        json.dumps(
            {
                "sync_fix_paths": [
                    "api/routers/admin_show_sync.py",
                    "/trr_backend/repositories/admin_show_reads.py",
                    "../ignored.py",
                    "",
                    123,
                ],
                "required_local_dirs": [
                    "scripts/socials/facebook",
                    "../ignored-dir",
                ],
            }
        ),
        encoding="utf-8",
    )

    paths, required_dirs = load_sync_fix_config(config_path)

    assert paths == (
        "api/routers/admin_show_sync.py",
        "trr_backend/repositories/admin_show_reads.py",
    )
    assert required_dirs == ("scripts/socials/facebook",)
