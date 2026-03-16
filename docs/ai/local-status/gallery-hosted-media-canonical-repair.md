# Gallery hosted-media canonical repair

Last updated: 2026-03-16

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-16
  current_phase: "backend phase complete"
  next_action: "If broader reachability remediation is needed beyond Bravo, rerun repair_gallery_hosts.py by source or show slice instead of probing the entire person-gallery candidate set in one pass"
  detail: self
```

- Canonical rebuild now rewrites legacy CloudFront gallery URLs for the relevant media and metadata tables.
- The Lisa Barlow Bravo asset now resolves on the canonical R2 media-variant URL.
