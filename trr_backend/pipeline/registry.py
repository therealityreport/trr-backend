"""Pipeline stage registry."""

from trr_backend.pipeline.stages import (
    collect,
    deploy,
    enrich,
    mirror,
    resolve,
    sync_screenalytics,
)

# Ordered list of pipeline stages
# Each tuple is (stage_name, stage_function)
STAGES = [
    ("01_collect", collect.run),
    ("02_resolve", resolve.run),
    ("03_enrich", enrich.run),
    ("04_mirror", mirror.run),
    ("05_deploy", deploy.run),
    ("06_sync_screenalytics", sync_screenalytics.run),
]
