import importlib
import sys

_mod = importlib.import_module("scripts.backfill.bootstrap_bravo_show_snapshots")

if __name__ == "__main__":
    raise SystemExit(_mod.main(sys.argv[1:]))
