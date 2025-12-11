from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
FEED_FILES_DIR = BASE_DIR / "feed_files"
FEED_FILES_DIR.mkdir(exist_ok=True)