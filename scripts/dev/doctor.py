#!/usr/bin/env python3
"""
Environment diagnostic tool for TRR-Backend.

Validates that the development environment is correctly configured:
- Python version is 3.11+
- Required dependencies are installed and importable
- No broken dependency graphs (pip check)
- Supabase package resolves to site-packages (not local directory)

Usage:
    python scripts/dev/doctor.py
    # or via make:
    make doctor
"""

from __future__ import annotations

import subprocess
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from pathlib import Path


def check_python_version() -> bool:
    """Check that Python version is 3.11+."""
    print(f"Python version: {sys.version}")
    major, minor = sys.version_info[:2]
    if major < 3 or (major == 3 and minor < 11):
        print(f"  FAIL: Python 3.11+ required, got {major}.{minor}")
        return False
    print(f"  OK: Python {major}.{minor} meets requirement (3.11+)")
    return True


def check_package(name: str, min_version: str | None = None) -> bool:
    """Check that a package is installed and meets minimum version."""
    try:
        installed_version = pkg_version(name)
        print(f"{name}: {installed_version}")

        if min_version:
            # Simple version comparison (works for most semver)
            installed_parts = [int(x) for x in installed_version.split(".")[:3]]
            required_parts = [int(x) for x in min_version.split(".")[:3]]
            # Pad shorter list
            while len(installed_parts) < 3:
                installed_parts.append(0)
            while len(required_parts) < 3:
                required_parts.append(0)

            if installed_parts < required_parts:
                print(f"  FAIL: {name} >= {min_version} required")
                return False

        print("  OK")
        return True
    except PackageNotFoundError:
        print(f"{name}: NOT INSTALLED")
        print(f"  FAIL: {name} is required. Run: pip install -r requirements.txt")
        return False


def check_supabase_location() -> bool:
    """Verify supabase package resolves to site-packages, not local directory."""
    try:
        import trr_backend.db.admin as supabase

        pkg_file = getattr(supabase, "__file__", None)
        if pkg_file is None:
            print("supabase.__file__: None (namespace package?)")
            print("  WARN: Cannot verify package location")
            return True

        pkg_path = Path(pkg_file).resolve()
        print(f"supabase.__file__: {pkg_path}")

        # Check if it's in site-packages
        if "site-packages" in str(pkg_path):
            print("  OK: Resolves to site-packages")
            return True
        else:
            print("  FAIL: supabase is NOT resolving to site-packages!")
            print("        This may cause import errors. Check PYTHONPATH.")
            return False
    except ImportError as e:
        print(f"supabase: IMPORT ERROR - {e}")
        print("  FAIL: Cannot import trr_backend.db.admin as supabase. Run: pip install -r requirements.txt")
        return False


def check_pip() -> bool:
    """Run pip check to detect broken dependencies."""
    print("Running pip check...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "check"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print("  OK: No broken requirements found")
        return True
    else:
        print("  FAIL: Broken requirements detected:")
        print(result.stdout)
        print(result.stderr)
        return False


def main() -> int:
    """Run all diagnostic checks."""
    print("=" * 60)
    print("TRR-Backend Environment Doctor")
    print("=" * 60)
    print()

    all_passed = True

    # Python version
    print("[1/5] Python Version")
    print("-" * 40)
    if not check_python_version():
        all_passed = False
    print()

    # Core packages
    print("[2/5] Core Packages")
    print("-" * 40)
    packages = [
        ("boto3", "1.35.0"),
        ("supabase", "2.0.0"),
        ("postgrest", "0.10.0"),
        ("fastapi", None),
        ("pytest", None),
    ]
    for name, min_ver in packages:
        if not check_package(name, min_ver):
            all_passed = False
    print()

    # Supabase location
    print("[3/5] Supabase Package Location")
    print("-" * 40)
    if not check_supabase_location():
        all_passed = False
    print()

    # pip check
    print("[4/5] Dependency Graph")
    print("-" * 40)
    if not check_pip():
        all_passed = False
    print()

    # Summary
    print("[5/5] Summary")
    print("-" * 40)
    if all_passed:
        print("ALL CHECKS PASSED")
        print()
        print("Your environment is correctly configured for TRR-Backend development.")
        return 0
    else:
        print("SOME CHECKS FAILED")
        print()
        print("To fix:")
        print("  1. Ensure Python 3.11+ is installed")
        print("  2. Activate your virtual environment: source .venv/bin/activate")
        print("  3. Install dependencies: pip install -r requirements.txt")
        print("  4. Verify no PYTHONPATH conflicts")
        return 1


if __name__ == "__main__":
    sys.exit(main())
