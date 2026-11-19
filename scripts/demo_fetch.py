"""End-to-end demo of the fetcher against a LOCAL MOCK — no network, no real data.

Serves tests/fixtures/demo_resource.json (25 synthetic rows) through a mocked
data.gov.in endpoint, three pages of 10, and writes the result into the real
raw zone under data/raw/demo-fixture/. The slug is deliberately NOT one of the
real registry datasets, and the payload says it is synthetic, so this artifact
can never be mistaken for fetched government data.

Usage:  python scripts/demo_fetch.py [raw_dir]      (default: data/raw)
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import responses

from bharatdata.ingest.fetcher import fetch_dataset
from bharatdata.registry import DatasetEntry

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "demo_resource.json"
DEMO_RESOURCE_ID = "00000000-0000-0000-0000-000000000000"  # obviously fake; mock only

ENTRY = DatasetEntry.model_validate(
    {
        "slug": "demo-fixture",
        "title": "Demo Fixture (synthetic)",
        "description": "Synthetic rows used to demonstrate the fetcher; not government data.",
        "sector": "demo",
        "ministry": "None (synthetic fixture)",
        "source_type": "api",
        "resource_id": DEMO_RESOURCE_ID,
        "resource_id_status": "VERIFIED",
        "source_url": "https://data.gov.in/",
        "format": "json",
        "refresh_cron": "0 0 * * *",
        "expected_schema": {"id": "string", "commodity": "string", "market": "string", "modal_price": "string"},
        "licence": "GODL-India",
        "attribution": "Synthetic demo data; no source.",
    }
)


def main(raw_dir: Path) -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    rows = fixture["records"]

    def serve(request):
        query = parse_qs(urlparse(request.url).query)
        offset, limit = int(query["offset"][0]), int(query["limit"][0])
        page = rows[offset : offset + limit]
        body = {
            "fixture": True,
            "note": fixture["note"],
            "total": len(rows),
            "count": len(page),
            "limit": str(limit),
            "offset": str(offset),
            "records": page,
        }
        return 200, {"Content-Type": "application/json"}, json.dumps(body).encode()

    url = re.compile(rf"^https://api\.data\.gov\.in/resource/{DEMO_RESOURCE_ID}(\?.*)?$")
    with responses.RequestsMock() as mock:  # any request not mocked raises: no real network
        mock.add_callback(responses.GET, url, callback=serve)
        result = fetch_dataset(ENTRY, api_key="demo-key-not-real", raw_dir=raw_dir, page_size=10)

    print(f"slug:            {result.slug}")
    print(f"run_id:          {result.run_id}")
    print(f"records fetched: {result.records_fetched} of {result.total_reported} (complete={result.complete})")
    for page in result.pages:
        print(f"  offset {page.offset:>3}  {page.record_count:>2} rows  sha256 {page.sha256[:16]}…  {page.raw_path}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else REPO_ROOT / "data" / "raw")
