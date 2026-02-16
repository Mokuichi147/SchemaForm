from __future__ import annotations

import base64
import csv
import io
from datetime import datetime, timezone
from typing import Any, Iterable

from schemaform.fields import (
    flatten_fields,
    flatten_filter_fields,
    format_array_group_value,
    get_nested_value,
)
from schemaform.protocols import FileRepository


def parse_bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "on", "yes"}


def normalize_number(value: Any, is_int: bool) -> Any:
    if value in (None, ""):
        return None
    try:
        return int(value) if is_int else float(value)
    except ValueError:
        return None


def ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_query_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def collect_file_ids(
    submissions: list[dict[str, Any]], fields: list[dict[str, Any]]
) -> set[str]:
    ids: set[str] = set()
    flat = flatten_fields(fields)
    file_keys = {f["flat_key"] for f in flat if f["type"] == "file"}
    for submission in submissions:
        data = submission.get("data_json", {})
        for key in file_keys:
            value = get_nested_value(data, key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        ids.add(item)
            elif isinstance(value, str):
                ids.add(value)
    return ids


def resolve_file_names(file_repo: FileRepository, file_ids: Iterable[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for file_id in file_ids:
        file_meta = file_repo.get_file(file_id)
        if file_meta:
            mapping[file_id] = file_meta.get("original_name", "")
    return mapping


def value_to_text(value: Any, file_names: dict[str, str], use_file_names: bool) -> str:
    if isinstance(value, list):
        return ", ".join(
            value_to_text(item, file_names, use_file_names) for item in value if item is not None
        )
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if use_file_names and isinstance(value, str) and value in file_names:
        return file_names[value]
    return str(value)


def apply_filters(
    submissions: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    query_params: dict[str, Any],
    file_names: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    q = str(query_params.get("q", "")).strip().lower()
    from_dt = parse_query_datetime(query_params.get("submitted_from"))
    to_dt = parse_query_datetime(query_params.get("submitted_to"))
    flat_fields = flatten_filter_fields(fields)
    resolved_file_names = file_names or {}

    def get_filter_values(current: Any, dotted_key: str) -> list[Any]:
        parts = dotted_key.split(".")

        def walk(node: Any, idx: int) -> list[Any]:
            if idx >= len(parts):
                if isinstance(node, list):
                    return list(node)
                return [node]

            key = parts[idx]
            values: list[Any] = []
            if isinstance(node, dict):
                if key in node:
                    values.extend(walk(node.get(key), idx + 1))
            elif isinstance(node, list):
                for item in node:
                    values.extend(walk(item, idx))
            return values

        return walk(current, 0)

    def iter_searchable_values(field_list: list[dict[str, Any]], current_data: Any) -> Iterable[str]:
        if not isinstance(current_data, dict):
            return
        for field in field_list:
            key = field.get("key")
            if not key or key not in current_data:
                continue
            value = current_data.get(key)
            field_type = field.get("type")
            is_array = bool(field.get("is_array"))

            if field_type == "group":
                children = field.get("children") or []
                if is_array:
                    if isinstance(value, list):
                        for item in value:
                            yield from iter_searchable_values(children, item)
                else:
                    yield from iter_searchable_values(children, value)
                continue

            if field_type == "file":
                if is_array:
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, str):
                                file_name = resolved_file_names.get(item, "")
                                if file_name:
                                    yield file_name
                elif isinstance(value, str):
                    file_name = resolved_file_names.get(value, "")
                    if file_name:
                        yield file_name
                continue

            if is_array:
                if isinstance(value, list):
                    for item in value:
                        if item not in (None, ""):
                            yield str(item)
            elif value not in (None, ""):
                yield str(value)

    def matches_free_text(data: dict[str, Any]) -> bool:
        if not q:
            return True
        combined = " ".join(iter_searchable_values(fields, data)).lower()
        return q in combined

    filtered: list[dict[str, Any]] = []
    for submission in submissions:
        created_at = submission.get("created_at")
        if created_at and (from_dt or to_dt):
            created_value = ensure_aware(created_at) if isinstance(created_at, datetime) else None
            if created_value is not None:
                if from_dt and created_value < ensure_aware(from_dt):
                    continue
                if to_dt and created_value > ensure_aware(to_dt):
                    continue
        data = submission.get("data_json", {})
        if not matches_free_text(data):
            continue
        ok = True
        for field in flat_fields:
            flat_key = field["flat_key"]
            param_key = f"f_{flat_key.replace('.', '__')}"
            values = get_filter_values(data, flat_key)
            value = values[0] if values else None
            field_type = field["type"]
            is_array = field.get("is_array", False)

            if field_type == "group":
                filter_value = str(query_params.get(param_key, "")).strip()
                if not filter_value:
                    continue
                if not any(filter_value.lower() in str(item).lower() for item in values):
                    ok = False
                    break
                continue

            if is_array:
                filter_value = str(query_params.get(param_key, "")).strip()
                if not filter_value:
                    continue
                filter_value_lower = filter_value.lower()
                items = [item for item in values if item not in (None, "")]
                if field_type == "enum":
                    if filter_value not in [str(item) for item in items]:
                        ok = False
                        break
                elif field_type == "file":
                    if not any(
                        filter_value_lower in resolved_file_names.get(item, "").lower()
                        for item in items
                        if isinstance(item, str)
                    ):
                        ok = False
                        break
                else:
                    if not any(filter_value_lower in str(item).lower() for item in items):
                        ok = False
                        break
                continue

            if field_type in {"string", "enum", "file", "datetime", "date", "time"}:
                filter_value = str(query_params.get(param_key, "")).strip()
                if not filter_value:
                    continue
                if field_type == "enum":
                    if str(value) != filter_value:
                        ok = False
                        break
                elif field_type == "file":
                    file_name = resolved_file_names.get(str(value), "")
                    if filter_value.lower() not in file_name.lower():
                        ok = False
                        break
                else:
                    if filter_value.lower() not in str(value or "").lower():
                        ok = False
                        break
            elif field_type in {"number", "integer"}:
                min_val = query_params.get(f"{param_key}_min")
                max_val = query_params.get(f"{param_key}_max")
                if min_val not in (None, "") or max_val not in (None, ""):
                    if value is None:
                        ok = False
                        break
                    if min_val not in (None, "") and value < float(min_val):
                        ok = False
                        break
                    if max_val not in (None, "") and value > float(max_val):
                        ok = False
                        break
            elif field_type == "boolean":
                filter_value = str(query_params.get(param_key, "")).strip().lower()
                if not filter_value:
                    continue
                expected = filter_value in {"1", "true", "on", "yes"}
                if bool(value) != expected:
                    ok = False
                    break
        if ok:
            filtered.append(submission)
    return filtered


def encode_cursor(created_at: datetime, submission_id: str) -> str:
    value = f"{ensure_aware(created_at).isoformat()}|{submission_id}"
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")


def decode_cursor(cursor: str) -> tuple[datetime, str] | None:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("utf-8")).decode("utf-8")
        created_at_raw, submission_id = raw.split("|", 1)
        created_at = datetime.fromisoformat(created_at_raw)
        return ensure_aware(created_at), submission_id
    except Exception:
        return None


def csv_headers_and_rows(
    fields: list[dict[str, Any]],
    submissions: list[dict[str, Any]],
    file_names: dict[str, str],
) -> tuple[list[str], list[list[str]]]:
    flat = flatten_fields(fields, expand_rows_for_group_arrays=True)
    max_lengths: dict[str, int] = {}

    for field in flat:
        if field.get("is_array") and field.get("type") != "group":
            fk = field["flat_key"]
            max_len = 1
            for submission in submissions:
                value = get_nested_value(submission.get("data_json", {}), fk)
                if isinstance(value, list):
                    max_len = max(max_len, len(value))
            max_lengths[fk] = max_len

    headers: list[str] = []
    for field in flat:
        fk = field["flat_key"]
        base = field["flat_label"]
        if field.get("type") == "group" and field.get("is_array"):
            headers.append(base)
        elif field.get("is_array"):
            for idx in range(max_lengths.get(fk, 1)):
                headers.append(f"{base}_{idx}")
        else:
            headers.append(base)

    rows: list[list[str]] = []
    for submission in submissions:
        data = submission.get("data_json", {})
        row: list[str] = []
        for field in flat:
            fk = field["flat_key"]
            value = get_nested_value(data, fk)
            is_file = field["type"] == "file"
            if field.get("type") == "group" and field.get("is_array"):
                row.append(format_array_group_value(value, field.get("children", [])))
            elif field.get("is_array"):
                items = value if isinstance(value, list) else []
                max_len = max_lengths.get(fk, 1)
                for idx in range(max_len):
                    item = items[idx] if idx < len(items) else None
                    row.append(value_to_text(item, file_names, is_file))
            else:
                row.append(value_to_text(value, file_names, is_file))
        rows.append(row)

    return headers, rows
