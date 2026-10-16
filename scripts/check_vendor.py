"""Verify unchanged pinned source inputs, without depending on the original clones."""

import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / "third_party_manifest.json").read_text())
count = 0
for dependency in manifest["dependencies"]:
    for relative, expected in dependency["files"].items():
        actual = hashlib.sha256((root / relative).read_bytes()).hexdigest()
        if actual != expected:
            raise SystemExit(f"vendor source differs: {relative}")
        count += 1
print(f"Verified {count} pinned upstream source inputs")
