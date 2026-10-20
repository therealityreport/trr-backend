# InsightFace initialization repair

Status: deployed and face inference verified in production on October 20, 2026.

## Cause and repair

Production v80 reproduced `InsightFace reference detector unavailable: unknown error`
in call `fc-01M5BX2X9ZFS8TZ238VN0EQ4G6`. Container
`ta-01M5BX2YAZH13ZKVHATD31YWJR` successfully downloaded the official antelopev2
archive, but all five ONNX files were under
`/root/.insightface/models/antelopev2/antelopev2/`. InsightFace 0.7.3 scans only
direct ONNX children of the profile directory. Direct construction raised its
message-less `assert 'detection' in self.models`.

Commit `d1ef24a8` provisions the same antelopev2 pack during vision image
construction. The helper verifies archive SHA256
`8e182f14fc6e80b3bfa375b33eb6cff7ee05d8ef7633e738d1c89021dcf0c5c5`,
copies the five exact named members into the expected directory, and requires
successful detection/recognition loading and CPU preparation. Failed provisioning
fails the image build. Runtime initialization now retains exception class,
profile and phase, including empty-message exceptions.

The model profile, 512-dimensional embedding contract, runtime provider defaults,
fallback policy and dependency versions remain unchanged.

## Source selection and deployment

- Maintained-source fix: `d1ef24a8`, on the existing
  `chore/repo-hygiene-2026-09-25` branch.
- Deployed candidate: production source
  `60cc311716881a7516f831d1ca763d55a9880733` plus the exact `d1ef24a8` patch.
- Newer ingestion work was reviewed before selection. Its explicit local-only
  release boundary remains in the October 19 deployment record and ingestion
  architecture record; this task does not publish those unrelated changes.
- Candidate archive contains no dirty checkout edits. Its complete file hash
  manifest is retained with the repair evidence.
- Modal workspace/profile `admin-56995`, environment `main`, app
  `trr-backend-jobs`, app ID `ap-DkLTRoSvqhbkGO7fHyHxrD`.
- Deployment **v81**, tag `insightface-d1ef24a8-on-v80`, October 20 at 05:42 UTC.
  Provider commit is null because deployment used the selected source archive;
  the base-plus-patch manifest is the exact source authority.
- Guarded deployment used `scripts/modal/deploy_backend.py --env main` with
  existing named runtime/social secrets. No secret updates occurred.
- Rollback remains the provider's **v80** artifact. Rollback was not needed.

## Verification

- Six focused initialization/archive regression tests passed.
- One affected Modal vision-image binding test passed.
- Scoped Ruff and whitespace checks passed.
- Independent candidate review found no blockers. Separate independent review
  confirmed the production-source selection boundary.
- Actual image build and inference in ephemeral app
  `ap-SNHlw4GfghHYsEx00TIqrY` detected six faces in InsightFace's `t1` sample
  with finite 512-dimensional embeddings. The app completed and stopped.
- Production explicit face-mode call `fc-01M5BXA384GSHT5B769F0W429D` returned
  six raw, filtered and retained faces, with the expected antelopev2 contract.
- Production default `faces_then_yolo` call `fc-01M5BXAMCD055B1Q4GHY7CS0V8`
  returned the same six faces plus person detections. Neither call returned
  `unavailable`.
- Both production calls used the official public sample:
  `https://raw.githubusercontent.com/deepinsight/insightface/master/python-package/insightface/data/images/t1.jpg`.
  No reference/candidate IDs were supplied and no application data was written.
- The deployment wrapper's `verify_modal_readiness.py --env main` passed.
  API `/health` returned HTTP 200 on its first canary attempt.
- Deployed API container `ta-01M5BXA5ZQSQA2666YFFZM204R` resolved dispatch to
  `trr-backend-jobs / main / run_admin_vision`.
- Prior passing checks for unchanged paths were reused; no full test suite or
  app build was repeated.

Firebase, admin credentials/access policy, survey setup and SQL ownership were
unchanged. TRR-APP build: not applicable (backend model initialization only).
This proves production face detection and model loading, not identity-matching
accuracy or browser click-through behavior.

Detailed nonsecret evidence is retained at
`/Users/thomashulihan/Documents/Codex/2026-10-20/insightface-repair/`.
The candidate archive and patch remain there for exact recovery. Deployment
used an archive, without a temporary Git branch or worktree.

## GitHub publication

The fix and this record were cherry-picked onto GitHub `main` at `c1ad6b1f`
in a separate publication worktree, producing `b320def8` and `f9bbf928`.
Only the six InsightFace implementation, regression-test and release-note paths
are included. The local-only ingestion commits are excluded.

Publication validation also changed the helper's image-only InsightFace import
to `import_module`, consistent with the existing vision runtime, because the
API/CI dependency lane does not install InsightFace. Formatting was normalized
for the repository's pinned Ruff version. These changes preserve model-loading
behavior; production v81 remains the deployed artifact recorded above.
