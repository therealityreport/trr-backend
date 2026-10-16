# Operator Chrome cookie environment

The Instagram and SocialBlade Chrome extraction helpers use pycookiecheat 0.8.0. Its upstream metadata requires `cryptography==43.*`, which conflicts with the patched API and Modal runtime lane. Keep extraction in a separate local Python 3.11 virtual environment. Never install its input or lock into `.venv`, an API/worker environment, or a Modal image.

From `TRR-Backend`:

```sh
python3.11 -m venv .venv-operator-cookies
.venv-operator-cookies/bin/python -m pip install -r requirements.operator-cookies.lock.txt
.venv-operator-cookies/bin/python scripts/modal/refresh_instagram_cookies_from_chrome.py --help
.venv-operator-cookies/bin/python scripts/socials/repair_socialblade_auth.py --help
```

The two `--help` commands check entrypoint availability without extracting cookies or contacting providers. Run an actual repair only within its existing authorization and confirmed Chrome account/profile requirements. Invoke the scripts directly with the operator interpreter; existing Make targets may select the runtime `.venv`.

The operator input includes both extractors' requests, Playwright, Scrapling and Modal dependencies. For an authorized repair that needs Playwright's browser fallback, install its browser in this environment using `.venv-operator-cookies/bin/python -m playwright install chromium`. Scrapling's browser setup may also be required by its selected fetcher; follow the existing browser procedure. Package import checks alone do not establish browser binary, keychain access, Chrome database compatibility or a valid account session.

Instagram remains validation-only by default. Local writes, Modal secret pushes, deployment and remote verification keep their existing explicit stage controls. SocialBlade retains its validation and Cloudflare-block handling. Missing extraction dependencies return a setup diagnostic before cookie extraction; the SocialBlade setup-error path does not attempt a live health probe. Both diagnostics direct operators to this separate environment.

The operator lock intentionally retains `cryptography==43.0.3` and its current advisory matches. This is a bounded compatibility exception, not a security-clean runtime. It handles local Chrome decryption and must not become a network service or general crypto environment. Revisit it when pycookiecheat supports a patched cryptography version or a reviewed replacement can preserve both helper workflows. Do not force-install cryptography 50 with `--no-deps` over the extractor's declared contract.

## Lock maintenance

Generate each runtime lock from its own input, preserving existing pins unless a minimum forces an update:

```sh
uv pip compile requirements.in --python-version 3.11 --python-platform x86_64-manylinux_2_28 -o requirements.lock.txt
uv pip compile requirements.modal.lean.in --python-version 3.11 --python-platform x86_64-manylinux_2_28 -o requirements.modal.lean.lock.txt
uv pip compile requirements.modal.browser.in --python-version 3.11 --python-platform x86_64-manylinux_2_28 -o requirements.modal.browser.lock.txt
uv pip compile requirements.modal.vision.in --python-version 3.11 --python-platform x86_64-manylinux_2_28 --index-strategy unsafe-best-match -o requirements.modal.vision.lock.txt
uv pip compile requirements.operator-cookies.in --python-version 3.11 --python-platform x86_64-manylinux_2_28 -o requirements.operator-cookies.lock.txt
```

The vision input retains the official PyTorch CPU index and its coordinated torch/torchvision pins. Do not combine the operator lock with the runtime locks. Validate changes in disposable environments, rerun the affected existing JWT/media/Modal/operator tests, and query every exact lane pin against current advisories. A changed lock prepares a source candidate; it does not update the active local environment or deployed Modal images.

Primary extractor contract: [pycookiecheat 0.8.0 on PyPI](https://pypi.org/project/pycookiecheat/0.8.0/). Runtime cryptography 49 still matched [GHSA-g6cj-pr64-35w5](https://github.com/advisories/GHSA-g6cj-pr64-35w5), fixed in 50.0.0; the runtime floor accounts for that refreshed advisory.
