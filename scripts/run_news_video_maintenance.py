import importlib
import sys

_mod = importlib.import_module("scripts.backfill.run_news_video_maintenance")

if __name__ == "__main__":
    raise SystemExit(_mod.main(sys.argv[1:]))
