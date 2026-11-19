"""scripts/demo_fetch.py must keep working: it produces the demo raw-zone artifact."""

from __future__ import annotations

import hashlib
import importlib.util
import json

from conftest import REPO_ROOT


def test_demo_script_writes_a_verifiable_raw_zone_artifact(tmp_path):
    spec = importlib.util.spec_from_file_location("demo_fetch", REPO_ROOT / "scripts" / "demo_fetch.py")
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)

    demo.main(tmp_path)

    directory = tmp_path / "demo-fixture"
    sidecars = sorted(directory.glob("*.meta.json"))
    assert len(sidecars) == 3
    total = 0
    for sidecar in sidecars:
        meta = json.loads(sidecar.read_text(encoding="utf-8"))
        payload = (directory / meta["payload_file"]).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == meta["sha256"]
        assert json.loads(payload)["fixture"] is True  # synthetic, and says so
        total += meta["record_count"]
    assert total == 25
