"""Dataset registry: one YAML file per dataset, validated on load.

Every dataset BharatData publishes is described by exactly one file in
``registry/``, named ``<slug>.yaml``. This module loads and validates all of
them and fails loudly: a single :class:`RegistryError` that names every
problem in every file, never a partial registry. A bad entry must stop here,
not reach the fetcher or the scheduler.

Rules enforced (the reasons are the point):

- Unknown keys and duplicate keys are errors, so a typo such as ``license:``
  cannot silently drop a field and PyYAML cannot silently keep "the last one".
- ``slug`` is URL-safe and equals the file name, so slugs are unique and the
  file for a dataset page is always findable.
- An ``api`` dataset either carries a data.gov.in resource UUID together with
  ``resource_id_status: VERIFIED``, or has ``resource_id: null`` together with
  ``resource_id_status: TODO_VERIFY``. There is no third state: a resource id
  is never guessed, and an unverified one is never fetched.
- ``expected_schema`` types come from a small fixed vocabulary the cleaning
  layer can rely on.
- ``refresh_cron`` is a 5-field cron expression (evaluated in UTC) limited to
  the grammar Celery Beat accepts, so the scheduler can use it unchanged.
- ``licence`` is a known licence id; ``attribution`` is mandatory. Both are
  part of every dataset's schema, not a footer.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

REGISTRY_DIR_ENV = "BHARATDATA_REGISTRY_DIR"
DEFAULT_REGISTRY_DIR = Path("registry")

ColumnType = Literal["string", "integer", "float", "date", "datetime", "boolean"]
SourceType = Literal["api", "file"]
SourceFormat = Literal["json", "csv", "xml"]
ResourceIdStatus = Literal["VERIFIED", "TODO_VERIFY"]
NonBlankStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# Licence id -> full name. A dataset under any other licence is a deliberate
# decision: add it here first.
LICENCES: dict[str, str] = {
    "GODL-India": "Government Open Data License – India (GODL)",
}

_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_CRON_ITEM_RE = re.compile(r"^(?:\*(?:/(?P<star_step>\d+))?|(?P<start>\d+)(?:-(?P<end>\d+)(?:/(?P<step>\d+))?)?)$")
# (name, lowest, highest) per cron field. Day of week is 0-6 with 0 = Sunday;
# 7 is rejected because Celery's crontab does not accept it.
_CRON_FIELDS = (
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day of month", 1, 31),
    ("month", 1, 12),
    ("day of week", 0, 6),
)


class RegistryError(Exception):
    """One or more registry files failed validation; ``problems`` lists them all."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = list(problems)
        super().__init__(
            "registry validation failed:\n" + "\n".join(f"  - {p}" for p in self.problems)
        )


def _check_cron(expression: str) -> None:
    """Raise ValueError unless ``expression`` is a valid 5-field cron expression."""
    fields = expression.split()
    if len(fields) != len(_CRON_FIELDS):
        raise ValueError(
            f"{expression!r} must have exactly 5 fields "
            "(minute hour day-of-month month day-of-week)"
        )
    for text, (name, lowest, highest) in zip(fields, _CRON_FIELDS):
        for item in text.split(","):
            match = _CRON_ITEM_RE.match(item)
            if match is None:
                raise ValueError(
                    f"{name} field {text!r} of {expression!r} is not valid; "
                    "use *, */n, a, a-b or a-b/n, comma-separated"
                )
            step = match["star_step"] or match["step"]
            if step is not None and int(step) < 1:
                raise ValueError(f"{name} field {text!r} of {expression!r} has a zero step")
            bounds = [int(v) for v in (match["start"], match["end"]) if v is not None]
            if any(v < lowest or v > highest for v in bounds):
                raise ValueError(
                    f"{name} field {text!r} of {expression!r} is outside {lowest}-{highest}"
                )
            if len(bounds) == 2 and bounds[0] > bounds[1]:
                raise ValueError(f"{name} field {text!r} of {expression!r} is a backwards range")


class DatasetEntry(BaseModel):
    """One dataset in the BharatData catalogue, exactly as its YAML declares it."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    slug: str
    title: NonBlankStr
    description: NonBlankStr
    sector: str
    ministry: NonBlankStr
    source_type: SourceType
    resource_id: str | None
    resource_id_status: ResourceIdStatus | None = None
    source_url: str
    format: SourceFormat
    refresh_cron: str
    expected_schema: dict[str, ColumnType] = Field(min_length=1)
    licence: str
    attribution: NonBlankStr

    @field_validator("slug", "sector")
    @classmethod
    def _kebab_case(cls, value: str) -> str:
        if not _SLUG_RE.match(value):
            raise ValueError(
                f"{value!r} must be lowercase letters/digits in words joined by single hyphens"
            )
        return value

    @field_validator("resource_id")
    @classmethod
    def _resource_id_is_a_uuid(cls, value: str | None) -> str | None:
        if value is not None and not _UUID_RE.match(value):
            raise ValueError(
                f"{value!r} is not a data.gov.in resource id "
                "(lowercase UUID, e.g. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"
            )
        return value

    @field_validator("source_url")
    @classmethod
    def _source_url_is_http(cls, value: str) -> str:
        parts = urlsplit(value)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise ValueError(f"{value!r} must be an absolute http(s) URL")
        return value

    @field_validator("refresh_cron")
    @classmethod
    def _cron_is_valid(cls, value: str) -> str:
        _check_cron(value)
        return value

    @field_validator("expected_schema")
    @classmethod
    def _columns_are_named(cls, value: dict[str, str]) -> dict[str, str]:
        if any(not column for column in value):
            raise ValueError("column names must not be blank")
        return value

    @field_validator("licence")
    @classmethod
    def _licence_is_known(cls, value: str) -> str:
        if value not in LICENCES:
            raise ValueError(f"unknown licence {value!r}; known licences: {sorted(LICENCES)}")
        return value

    @model_validator(mode="after")
    def _source_is_honest(self) -> DatasetEntry:
        if self.source_type == "api":
            if self.format != "json":
                raise ValueError(
                    f"api sources are fetched as JSON, so format must be 'json', not {self.format!r}"
                )
            if self.resource_id is None and self.resource_id_status != "TODO_VERIFY":
                raise ValueError(
                    "api dataset has no resource_id, so it must declare "
                    "resource_id_status: TODO_VERIFY — a data.gov.in resource UUID is never guessed"
                )
            if self.resource_id is not None and self.resource_id_status != "VERIFIED":
                raise ValueError(
                    "resource_id is set, so resource_id_status must be VERIFIED — set it only "
                    "after confirming the UUID and expected_schema against the live resource"
                )
        else:
            if self.resource_id is not None or self.resource_id_status is not None:
                raise ValueError(
                    "file sources are downloaded from source_url: resource_id must be null "
                    "and resource_id_status must be omitted"
                )
        return self

    @property
    def is_fetchable(self) -> bool:
        """True when the API fetcher may run: an api source with a verified resource id."""
        return self.source_type == "api" and self.resource_id is not None

    @property
    def licence_name(self) -> str:
        """Full name of the licence, for attribution text."""
        return LICENCES[self.licence]


class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys (PyYAML silently keeps the last)."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
        seen: set[Any] = set()
        for key_node, _value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                if key in seen:
                    raise yaml.constructor.ConstructorError(
                        None, None, f"duplicate key {key!r}", key_node.start_mark
                    )
                seen.add(key)
            except TypeError:
                pass  # unhashable key: the parent constructor reports it
        return super().construct_mapping(node, deep=deep)


def _describe(error: dict[str, Any]) -> str:
    """One pydantic error as ``field: message``."""
    location = ".".join(str(part) for part in error["loc"]) or "entry"
    message = error["msg"].removeprefix("Value error, ")
    return f"{location}: {message}"


def load_entry(path: str | Path) -> DatasetEntry:
    """Load and validate one registry file.

    Raises :class:`RegistryError` listing every problem in the file: unreadable
    YAML, missing/unknown/invalid fields, or a slug that differs from the file
    name.
    """
    path = Path(path)
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = yaml.load(handle, Loader=_UniqueKeyLoader)  # a SafeLoader subclass
    except (OSError, UnicodeDecodeError) as exc:
        raise RegistryError([f"{path.name}: cannot read file: {exc}"]) from exc
    except yaml.YAMLError as exc:
        raise RegistryError([f"{path.name}: invalid YAML: {' '.join(str(exc).split())}"]) from exc

    if not isinstance(raw, dict):
        found = "an empty file" if raw is None else f"a {type(raw).__name__}"
        raise RegistryError([f"{path.name}: expected a mapping of fields, found {found}"])

    try:
        entry = DatasetEntry.model_validate(raw)
    except ValidationError as exc:
        raise RegistryError([f"{path.name}: {_describe(e)}" for e in exc.errors()]) from exc

    if entry.slug != path.stem:
        raise RegistryError(
            [f"{path.name}: slug {entry.slug!r} must equal the file name ({path.stem!r})"]
        )
    return entry


def registry_dir(directory: str | Path | None = None) -> Path:
    """The registry directory: the argument, else $BHARATDATA_REGISTRY_DIR, else ./registry."""
    if directory is not None:
        return Path(directory)
    return Path(os.environ.get(REGISTRY_DIR_ENV) or DEFAULT_REGISTRY_DIR)


def load_registry(directory: str | Path | None = None) -> dict[str, DatasetEntry]:
    """Load and validate every ``*.yaml`` file in the registry directory.

    Returns ``{slug: entry}`` ordered by slug. Raises :class:`RegistryError`
    naming every problem found across all files — a registry either loads
    completely or not at all.
    """
    root = registry_dir(directory)
    if not root.is_dir():
        raise RegistryError([f"registry directory not found: {root}"])

    problems = [
        f"{stray.name}: registry files must use the .yaml extension"
        for stray in sorted(root.glob("*.yml"))
    ]
    files = sorted(root.glob("*.yaml"))
    if not files and not problems:
        problems.append(f"no registry files (*.yaml) in {root}")

    entries: dict[str, DatasetEntry] = {}
    for path in files:
        try:
            entry = load_entry(path)
        except RegistryError as exc:
            problems.extend(exc.problems)
        else:
            entries[entry.slug] = entry

    if problems:
        raise RegistryError(problems)
    return entries
