"""Registry loader: all five real entries load; broken entries are rejected loudly."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from conftest import REGISTRY_DIR
from bharatdata.registry import DatasetEntry, RegistryError, load_entry, load_registry

FIRST_FIVE = {
    "daily-mandi-prices-agmarknet",
    "realtime-aqi-by-station",
    "petrol-diesel-prices-by-city",
    "imd-rainfall",
    "pincode-directory",
}


def test_loads_all_five_real_entries():
    registry = load_registry(REGISTRY_DIR)
    assert set(registry) == FIRST_FIVE
    assert all(isinstance(entry, DatasetEntry) for entry in registry.values())


def test_real_entries_never_claim_an_unverified_resource_id():
    """No UUID may be invented: until verified, every entry is null + TODO_VERIFY."""
    for entry in load_registry(REGISTRY_DIR).values():
        assert entry.resource_id is None, entry.slug
        assert entry.resource_id_status == "TODO_VERIFY", entry.slug
        assert not entry.is_fetchable, entry.slug


def test_real_entries_carry_licence_and_attribution():
    for entry in load_registry(REGISTRY_DIR).values():
        assert entry.licence == "GODL-India"
        assert "GODL" in entry.attribution and "BharatData" in entry.attribution
        assert entry.expected_schema, entry.slug


def _write(directory: Path, name: str, data: dict) -> Path:
    path = directory / f"{name}.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


def _good_fields() -> dict:
    return yaml.safe_load((REGISTRY_DIR / "imd-rainfall.yaml").read_text(encoding="utf-8"))


def test_rejects_a_deliberately_broken_entry(tmp_path):
    broken = _good_fields()
    del broken["attribution"]  # required
    broken["refresh_cron"] = "every day"  # not a cron expression
    broken["expected_schema"] = {"state": "text"}  # type outside the vocabulary
    broken["licence_typo"] = "x"  # unknown key
    _write(tmp_path, "imd-rainfall", broken)

    with pytest.raises(RegistryError) as excinfo:
        load_registry(tmp_path)

    message = str(excinfo.value)
    # Every problem is reported, each tied to its file and field.
    assert "imd-rainfall.yaml: attribution" in message
    assert "imd-rainfall.yaml: refresh_cron" in message
    assert "imd-rainfall.yaml: expected_schema" in message
    assert "imd-rainfall.yaml: licence_typo" in message
    assert len(excinfo.value.problems) >= 4


@pytest.mark.parametrize(
    ("change", "expected_fragment"),
    [
        ({"resource_id": "not-a-uuid", "resource_id_status": "VERIFIED"}, "resource_id"),
        # A UUID present but never verified: this is how a guessed id would sneak in.
        ({"resource_id": "11111111-2222-3333-4444-555555555555"}, "VERIFIED"),
        ({"resource_id_status": None}, "TODO_VERIFY"),
        ({"licence": "MIT"}, "unknown licence"),
        ({"slug": "Bad Slug"}, "slug"),
        ({"slug": "some-other-slug"}, "must equal the file name"),
        ({"source_url": "ftp://example.org/x"}, "source_url"),
        ({"refresh_cron": "61 * * * *"}, "outside 0-59"),
        ({"refresh_cron": "* * * *"}, "5 fields"),
        ({"expected_schema": {}}, "expected_schema"),
        ({"source_type": "scrape"}, "source_type"),
        ({"format": "csv"}, "format must be 'json'"),
        ({"attribution": "   "}, "attribution"),
    ],
)
def test_rejects_each_kind_of_bad_value(tmp_path, change, expected_fragment):
    fields = _good_fields()
    fields.update(change)
    path = _write(tmp_path, "imd-rainfall", fields)
    with pytest.raises(RegistryError, match=expected_fragment):
        load_entry(path)


def test_one_bad_file_fails_the_whole_registry(tmp_path):
    shutil.copy(REGISTRY_DIR / "pincode-directory.yaml", tmp_path)
    bad = _good_fields()
    bad["sector"] = "NOT KEBAB"
    _write(tmp_path, "imd-rainfall", bad)
    with pytest.raises(RegistryError, match="imd-rainfall.yaml"):
        load_registry(tmp_path)


def test_duplicate_yaml_keys_are_an_error(tmp_path):
    text = (REGISTRY_DIR / "imd-rainfall.yaml").read_text(encoding="utf-8") + "\ntitle: Second Title\n"
    (tmp_path / "imd-rainfall.yaml").write_text(text, encoding="utf-8")
    with pytest.raises(RegistryError, match="duplicate key 'title'"):
        load_entry(tmp_path / "imd-rainfall.yaml")


def test_unreadable_shapes_are_errors(tmp_path):
    (tmp_path / "empty.yaml").write_text("", encoding="utf-8")
    (tmp_path / "list.yaml").write_text("- a\n- b\n", encoding="utf-8")
    (tmp_path / "wrong-ext.yml").write_text("slug: x\n", encoding="utf-8")
    with pytest.raises(RegistryError) as excinfo:
        load_registry(tmp_path)
    text = str(excinfo.value)
    assert "empty.yaml: expected a mapping" in text
    assert "list.yaml: expected a mapping" in text
    assert "wrong-ext.yml: registry files must use the .yaml extension" in text


def test_missing_or_empty_registry_directory_fails(tmp_path):
    with pytest.raises(RegistryError, match="not found"):
        load_registry(tmp_path / "nope")
    with pytest.raises(RegistryError, match="no registry files"):
        load_registry(tmp_path)


def test_a_verified_entry_is_fetchable(tmp_path):
    fields = _good_fields()
    fields["resource_id"] = "00000000-0000-0000-0000-000000000000"
    fields["resource_id_status"] = "VERIFIED"
    entry = load_entry(_write(tmp_path, "imd-rainfall", fields))
    assert entry.is_fetchable
    assert entry.licence_name.startswith("Government Open Data License")
