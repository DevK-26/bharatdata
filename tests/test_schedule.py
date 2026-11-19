"""Celery Beat schedule: generated from registry/*.yaml, never hand-written."""

from __future__ import annotations

import shutil
from datetime import datetime, timezone

import pytest
import yaml
from celery import Celery
from celery.beat import Scheduler
from celery.schedules import crontab
from freezegun import freeze_time

from conftest import REGISTRY_DIR, REPO_ROOT, make_entry
from bharatdata.ingest.schedule import ENTRY_PREFIX, FETCH_TASK_NAME, build_beat_schedule, cron_to_schedule
from bharatdata.ingest.worker import celery_app, install_beat_schedule
from bharatdata.registry import RegistryError, load_registry


def _yaml_crons(directory=REGISTRY_DIR) -> dict[str, str]:
    """slug -> refresh_cron read straight from the YAML text, bypassing our loader."""
    crons = {}
    for path in sorted(directory.glob("*.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        crons[data["slug"]] = data["refresh_cron"]
    return crons


def _crontab_for(expression: str) -> crontab:
    m, h, dom, mon, dow = expression.split()
    return crontab(minute=m, hour=h, day_of_month=dom, month_of_year=mon, day_of_week=dow)


def test_one_beat_entry_per_registry_dataset_with_the_yaml_cron():
    registry = load_registry(REGISTRY_DIR)
    schedule = build_beat_schedule(registry)
    crons = _yaml_crons()

    assert len(crons) == 5
    assert set(schedule) == {f"{ENTRY_PREFIX}{slug}" for slug in crons}  # exactly one each, nothing extra
    for slug, cron in crons.items():
        entry = schedule[f"{ENTRY_PREFIX}{slug}"]
        assert entry["task"] == FETCH_TASK_NAME
        assert entry["args"] == (slug,)
        assert entry["schedule"] == _crontab_for(cron)


def test_known_crons_decode_to_the_expected_fields():
    """Pin one real entry's decoded fields, so a parser bug cannot hide behind a symmetric helper."""
    imd = build_beat_schedule(load_registry(REGISTRY_DIR))["fetch-imd-rainfall"]["schedule"]
    assert _yaml_crons()["imd-rainfall"] == "0 3 * * 1"
    assert (imd.minute, imd.hour, imd.day_of_week) == ({0}, {3}, {1})
    assert imd.day_of_month == set(range(1, 32)) and imd.month_of_year == set(range(1, 13))


def test_the_worker_app_installs_the_generated_schedule():
    assert celery_app.conf.beat_schedule == build_beat_schedule(load_registry(REGISTRY_DIR))
    assert celery_app.conf.timezone == "UTC"


def test_no_dataset_is_hand_scheduled_in_source():
    """Slugs appear in registry/ only; no source file may name a dataset or hard-code a cron."""
    sources = {p: p.read_text(encoding="utf-8") for p in (REPO_ROOT / "src").rglob("*.py")}
    for slug in _yaml_crons():
        for path, text in sources.items():
            assert slug not in text, f"{path} hard-codes dataset slug {slug!r}"
    assert [p.name for p, t in sources.items() if "crontab(" in t] == ["schedule.py"]


# --- proof the schedule comes from the YAML, not from constants --------------


def _copy_registry(tmp_path):
    target = tmp_path / "registry"
    shutil.copytree(REGISTRY_DIR, target)
    return target


def test_changing_one_yaml_cron_changes_exactly_that_entry(tmp_path):
    original = build_beat_schedule(load_registry(REGISTRY_DIR))
    directory = _copy_registry(tmp_path)
    path = directory / "pincode-directory.yaml"
    text = path.read_text(encoding="utf-8")
    old = _yaml_crons()["pincode-directory"]
    assert f'refresh_cron: "{old}"' in text
    path.write_text(text.replace(f'refresh_cron: "{old}"', 'refresh_cron: "17 4 * * 3"'), encoding="utf-8")

    changed = build_beat_schedule(load_registry(directory))

    assert changed["fetch-pincode-directory"]["schedule"] == _crontab_for("17 4 * * 3")
    assert changed["fetch-pincode-directory"]["schedule"] != original["fetch-pincode-directory"]["schedule"]
    for name in set(original) - {"fetch-pincode-directory"}:
        assert changed[name] == original[name]
    # ...and a test that asserts the OLD cron against this registry would catch it:
    assert changed["fetch-pincode-directory"]["schedule"] != _crontab_for(old)


@pytest.mark.parametrize("bad_cron", ["61 * * * *", "* * * *", "0 3 * * 7", "*/0 * * * *", "every monday"])
def test_a_wrong_cron_in_the_registry_is_rejected_not_scheduled(tmp_path, bad_cron):
    directory = _copy_registry(tmp_path)
    path = directory / "imd-rainfall.yaml"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace('refresh_cron: "0 3 * * 1"', f'refresh_cron: "{bad_cron}"'), encoding="utf-8")
    with pytest.raises(RegistryError, match="refresh_cron"):
        build_beat_schedule(load_registry(directory))


def test_worker_start_up_fails_loudly_on_a_broken_registry(tmp_path, monkeypatch):
    directory = _copy_registry(tmp_path)
    (directory / "imd-rainfall.yaml").write_text("slug: imd-rainfall\n", encoding="utf-8")
    monkeypatch.setenv("BHARATDATA_REGISTRY_DIR", str(directory))
    with pytest.raises(RegistryError):
        install_beat_schedule(Celery("scratch"))


# --- the clock: a scheduled task fires at its cron time -----------------------


def _fresh_beat_app(registry) -> tuple[Celery, list[tuple[str, datetime]]]:
    """A private Celery app whose fetch task records (slug, time) instead of fetching."""
    app = Celery("beat-clock-test", broker="memory://", set_as_current=False)
    app.conf.task_always_eager = True
    app.conf.timezone = "UTC"
    app.conf.enable_utc = True
    fired: list[tuple[str, datetime]] = []

    @app.task(name=FETCH_TASK_NAME, shared=False)
    def fetch(slug):
        fired.append((slug, datetime.now(timezone.utc)))

    install_beat_schedule(app, registry)
    return app, fired


def _tick(scheduler: Scheduler) -> None:
    """Run Beat's loop body until it has nothing more to do right now.

    ``Scheduler.tick`` runs at most ONE due task per call and returns 0 when it ran
    one; a real beat process keeps calling it, and so do we.
    """
    for _ in range(100):
        if scheduler.tick() != 0:
            return
    raise AssertionError("beat kept firing tasks without settling")


def test_scheduled_task_fires_at_the_cron_time_and_not_before_or_twice():
    registry = {"fixture-dataset": make_entry(refresh_cron="30 2 * * *")}  # daily 02:30 UTC
    with freeze_time("2026-11-19 02:29:40") as clock:
        app, fired = _fresh_beat_app(registry)
        scheduler = Scheduler(app=app)

        _tick(scheduler)
        assert fired == []  # 02:29:40 — not yet

        clock.move_to("2026-11-19 02:29:59")
        _tick(scheduler)
        assert fired == []

        clock.move_to("2026-11-19 02:30:00")
        _tick(scheduler)
        assert fired == [("fixture-dataset", datetime(2026, 11, 19, 2, 30, tzinfo=timezone.utc))]

        _tick(scheduler)  # same instant again: must not double-fire
        clock.move_to("2026-11-19 02:45:00")
        _tick(scheduler)
        assert len(fired) == 1

        clock.move_to("2026-11-20 02:30:00")  # the next day
        _tick(scheduler)
        assert [t for _, t in fired] == [
            datetime(2026, 11, 19, 2, 30, tzinfo=timezone.utc),
            datetime(2026, 11, 20, 2, 30, tzinfo=timezone.utc),
        ]


def test_each_real_registry_dataset_fires_at_its_own_yaml_cron():
    """Weekly imd-rainfall ("0 3 * * 1") fires on Monday 03:00 UTC and on no other day."""
    registry = load_registry(REGISTRY_DIR)
    monday = "2026-11-23"
    assert datetime.fromisoformat(monday).weekday() == 0
    with freeze_time("2026-11-22 03:00:00") as clock:  # Sunday 03:00 — wrong day for imd
        app, fired = _fresh_beat_app(registry)
        scheduler = Scheduler(app=app)
        _tick(scheduler)
        assert "imd-rainfall" not in [slug for slug, _ in fired]

        clock.move_to(f"{monday} 02:59:59")
        _tick(scheduler)
        assert "imd-rainfall" not in [slug for slug, _ in fired]

        clock.move_to(f"{monday} 03:00:00")
        _tick(scheduler)
        assert [slug for slug, _ in fired].count("imd-rainfall") == 1

        clock.move_to("2026-11-24 03:00:00")  # Tuesday: weekly dataset must stay quiet
        _tick(scheduler)
        assert [slug for slug, _ in fired].count("imd-rainfall") == 1

        clock.move_to("2026-11-30 03:00:00")  # next Monday
        _tick(scheduler)
        assert [slug for slug, _ in fired].count("imd-rainfall") == 2


def test_cron_to_schedule_rejects_a_non_five_field_expression():
    with pytest.raises(ValueError, match="5 fields"):
        cron_to_schedule("0 3 * *")
