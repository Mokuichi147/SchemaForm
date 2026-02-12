from __future__ import annotations

from typing import Any

from schemaform.utils import dumps_json


def flatten_fields(
    fields: list[dict[str, Any]],
    prefix: str = "",
    label_prefix: str = "",
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for field in fields:
        key = f"{prefix}{field['key']}" if prefix else field["key"]
        label = f"{label_prefix}{field.get('label') or field['key']}" if label_prefix else (field.get("label") or field["key"])
        if field.get("type") == "group":
            if field.get("is_array"):
                result.append({**field, "flat_key": key, "flat_label": label})
            else:
                children = field.get("children") or []
                result.extend(flatten_fields(children, prefix=key + ".", label_prefix=label + "."))
        else:
            result.append({**field, "flat_key": key, "flat_label": label})
    return result


def _build_child_map(children: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for child in children:
        mapping[child["key"]] = child
    return mapping


def _label_for_field(field: dict[str, Any], fallback_key: str) -> str:
    return field.get("label") or field.get("key") or fallback_key


def _format_value_by_field(value: Any, field: dict[str, Any]) -> Any:
    if field.get("type") != "group":
        return value
    children = field.get("children") or []
    if field.get("is_array"):
        if not isinstance(value, list):
            return value
        return [
            _format_group_item(item, children)
            if isinstance(item, dict)
            else item
            for item in value
        ]
    if isinstance(value, dict):
        return _format_group_item(value, children)
    return value


def _format_group_item(item: dict[str, Any], children: list[dict[str, Any]]) -> dict[str, Any]:
    child_map = _build_child_map(children)
    formatted: dict[str, Any] = {}
    for key, raw_value in item.items():
        child = child_map.get(key)
        if child:
            label = _label_for_field(child, key)
            formatted[label] = _format_value_by_field(raw_value, child)
        else:
            formatted[key] = raw_value
    return formatted


def format_array_group_value(value: Any, children: list[dict[str, Any]]) -> str:
    if not value or not isinstance(value, list):
        return ""
    result: list[Any] = []
    for item in value:
        if isinstance(item, dict):
            result.append(_format_group_item(item, children))
        else:
            result.append(item)
    return dumps_json(result)


def get_nested_value(data: dict[str, Any], dotted_key: str) -> Any:
    parts = dotted_key.split(".")
    current: Any = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def set_nested_value(data: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    current = data
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def clean_empty_recursive(data: Any) -> Any:
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            result = clean_empty_recursive(v)
            if result is not None and result != "":
                cleaned[k] = result
        return cleaned if cleaned else None
    if isinstance(data, list):
        cleaned_list = []
        for item in data:
            result = clean_empty_recursive(item)
            if result is not None and result != "":
                cleaned_list.append(result)
        return cleaned_list if cleaned_list else None
    return data
