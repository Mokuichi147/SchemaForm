from __future__ import annotations

from typing import Any

import orjson

from schemaform.config import ALLOWED_TYPES, KEY_PATTERN
from schemaform.file_formats import (
    normalize_allowed_extensions,
    normalize_file_format,
    parse_allowed_extensions,
)
from schemaform.utils import generate_field_key, now_utc, to_iso


def parse_fields_json(fields_json: str) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    try:
        raw_fields = orjson.loads(fields_json) if fields_json else []
    except orjson.JSONDecodeError:
        return [], ["フィールド定義の解析に失敗しました"]

    seen_keys: set[str] = set()

    def _parse_recursive(raw_list: list, prefix: str = "") -> list[dict[str, Any]]:
        fields: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_list, start=1):
            loc = f"{prefix}{index}行目" if prefix else f"{index}行目"
            key = str(raw.get("key", "")).strip()
            label = str(raw.get("label", "")).strip()

            if not label:
                errors.append(f"{loc}: ラベルは必須です")

            if not key:
                key = generate_field_key(seen_keys)
            if not KEY_PATTERN.match(key):
                errors.append(f"{loc}: キーは英字で始まり英数字/アンダースコアのみです")
            if key in seen_keys:
                errors.append(f"{loc}: キーが重複しています ({key})")
            else:
                seen_keys.add(key)

            field_type = str(raw.get("type", "")).strip()
            is_array = bool(raw.get("is_array"))
            items_type = str(raw.get("items_type", "")).strip() if is_array else ""
            expand_rows = bool(raw.get("expand_rows")) if (field_type == "group" and is_array) else False
            master_form_id = str(raw.get("master_form_id", "")).strip() if field_type == "master" else ""
            master_label_key = str(raw.get("master_label_key", "")).strip() if field_type == "master" else ""
            raw_format = str(raw.get("format", "")).strip()
            if field_type == "string":
                format_value = raw_format if raw_format in {"", "email", "url"} else ""
                if raw_format and not format_value:
                    errors.append(f"{loc}: 文字列フォーマットが不正です ({raw_format})")
            elif field_type == "file":
                format_value = normalize_file_format(raw_format)
                if raw_format and not format_value:
                    errors.append(f"{loc}: ファイル分類が不正です ({raw_format})")
            else:
                format_value = ""
            allowed_extensions: list[str] = []
            if field_type == "file":
                allowed_extensions, invalid_extensions = parse_allowed_extensions(
                    raw.get("allowed_extensions")
                )
                if invalid_extensions:
                    samples = ", ".join(invalid_extensions[:3])
                    errors.append(f"{loc}: 許可拡張子が不正です ({samples})")
            master_display_fields: list[str] = []
            if field_type == "master":
                raw_display_fields = raw.get("master_display_fields") or []
                if isinstance(raw_display_fields, list):
                    seen_display_keys: set[str] = set()
                    for item in raw_display_fields:
                        key_name = str(item).strip()
                        if not key_name or key_name in seen_display_keys:
                            continue
                        seen_display_keys.add(key_name)
                        master_display_fields.append(key_name)

            if field_type not in ALLOWED_TYPES:
                errors.append(f"{loc}: 種類が不正です ({field_type})")
            if field_type == "master" and not master_form_id:
                errors.append(f"{loc}: 参照元フォームを指定してください")

            children: list[dict[str, Any]] = []
            if field_type == "group":
                raw_children = raw.get("children") or []
                children = _parse_recursive(raw_children, prefix=f"{loc}.")
                if not children and not errors:
                    errors.append(f"{loc}: グループには子フィールドが必要です")
            else:
                if is_array and items_type not in ALLOWED_TYPES:
                    errors.append(f"{loc}: 配列の要素型が不正です ({items_type})")

            enum_values = [
                value.strip()
                for value in (raw.get("enum") or [])
                if isinstance(value, str) and value.strip()
            ]
            if (field_type == "enum" or items_type == "enum") and not enum_values:
                errors.append(f"{loc}: enumは値を指定してください")

            min_raw = str(raw.get("min", "")).strip()
            max_raw = str(raw.get("max", "")).strip()
            min_value = float(min_raw) if min_raw else None
            max_value = float(max_raw) if max_raw else None

            fields.append(
                {
                    "key": key,
                    "label": label,
                    "type": field_type,
                    "required": bool(raw.get("required")),
                    "description": str(raw.get("description", "")).strip(),
                    "placeholder": str(raw.get("placeholder", "")).strip(),
                    "enum": enum_values,
                    "min": min_value,
                    "max": max_value,
                    "format": format_value,
                    "allowed_extensions": allowed_extensions,
                    "is_array": is_array,
                    "items_type": items_type,
                    "multiline": bool(raw.get("multiline")),
                    "expand_rows": expand_rows,
                    "master_form_id": master_form_id,
                    "master_label_key": master_label_key,
                    "master_display_fields": master_display_fields,
                    "children": children,
                }
            )
        return fields

    fields = _parse_recursive(raw_fields)

    if not fields:
        errors.append("最低1つのフィールドが必要です")

    return fields, errors


def build_property(field: dict[str, Any]) -> dict[str, Any]:
    def build_item(item_type: str) -> dict[str, Any]:
        if item_type == "file":
            payload: dict[str, Any] = {"type": "string", "format": "binary"}
            file_format = normalize_file_format(field.get("format"))
            if file_format:
                payload["x-file-format"] = file_format
            allowed_extensions = normalize_allowed_extensions(field.get("allowed_extensions"))
            if allowed_extensions:
                payload["x-file-extensions"] = allowed_extensions
            return payload
        if item_type == "datetime":
            return {"type": "string", "format": "datetime-local"}
        if item_type == "date":
            return {"type": "string", "format": "date"}
        if item_type == "time":
            return {"type": "string", "format": "time"}
        if item_type == "enum":
            return {"type": "string", "enum": field.get("enum", [])}
        if item_type == "master":
            payload: dict[str, Any] = {"type": "string", "x-field-type": "master"}
            if field.get("master_form_id"):
                payload["x-master-form-id"] = field["master_form_id"]
            if field.get("master_label_key"):
                payload["x-master-label-key"] = field["master_label_key"]
            if field.get("master_display_fields"):
                payload["x-master-display-fields"] = field["master_display_fields"]
            return payload
        payload: dict[str, Any] = {"type": item_type}
        if item_type in {"number", "integer"}:
            if field.get("min") is not None:
                payload["minimum"] = field["min"]
            if field.get("max") is not None:
                payload["maximum"] = field["max"]
        if item_type == "string" and field.get("format"):
            payload["format"] = field["format"]
        return payload

    if field["type"] == "group":
        children = field.get("children") or []
        child_schema, child_order = schema_from_fields(children)
        obj: dict[str, Any] = {
            "type": "object",
            "properties": child_schema.get("properties", {}),
            "x-field-type": "group",
            "x-field-order": child_order,
        }
        if child_schema.get("required"):
            obj["required"] = child_schema["required"]
        if field.get("is_array"):
            prop: dict[str, Any] = {"type": "array", "items": obj}
            if field.get("expand_rows"):
                prop["x-expand-rows"] = True
        else:
            prop = obj
    elif field["is_array"]:
        item_type = field.get("items_type") or "string"
        prop = {"type": "array", "items": build_item(item_type)}
    else:
        prop = build_item(field["type"])

    prop["title"] = field.get("label") or field["key"]
    if field.get("description"):
        prop["description"] = field["description"]
    if field.get("placeholder"):
        prop["x-placeholder"] = field["placeholder"]
    if field.get("multiline"):
        prop["x-multiline"] = True

    return prop


def normalize_field_order(schema: dict[str, Any], field_order: list[str] | None) -> list[str]:
    properties = list(schema.get("properties", {}).keys())
    if not field_order:
        return properties
    seen: set[str] = set()
    ordered: list[str] = []
    for key in field_order:
        if key in properties and key not in seen:
            ordered.append(key)
            seen.add(key)
    for key in properties:
        if key not in seen:
            ordered.append(key)
    return ordered


def sanitize_form_output(form: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": form["id"],
        "public_id": form["public_id"],
        "name": form.get("name", ""),
        "description": form.get("description", ""),
        "status": form.get("status", "inactive"),
        "schema_json": form.get("schema_json", {}),
        "field_order": form.get("field_order", []),
        "webhook_url": form.get("webhook_url", ""),
        "webhook_on_submit": bool(form.get("webhook_on_submit")),
        "webhook_on_delete": bool(form.get("webhook_on_delete")),
        "created_at": to_iso(form.get("created_at", now_utc())),
        "updated_at": to_iso(form.get("updated_at", now_utc())),
    }


def schema_from_fields(fields: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    properties: dict[str, Any] = {}
    required: list[str] = []
    field_order: list[str] = []
    for field in fields:
        key = field["key"]
        field_order.append(key)
        properties[key] = build_property(field)
        if field.get("required"):
            required.append(key)
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema, field_order


def fields_from_schema(schema: dict[str, Any], field_order: list[str]) -> list[dict[str, Any]]:
    properties = schema.get("properties", {})
    order = field_order or list(properties.keys())
    fields: list[dict[str, Any]] = []

    for key in order:
        prop = properties.get(key, {})
        is_array = prop.get("type") == "array"
        target = prop.get("items", {}) if is_array else prop
        raw_master_display_fields = target.get("x-master-display-fields", [])
        master_display_fields = (
            [str(item).strip() for item in raw_master_display_fields if str(item).strip()]
            if isinstance(raw_master_display_fields, list)
            else []
        )

        is_group = (
            target.get("x-field-type") == "group"
            or (target.get("type") == "object" and "properties" in target)
        )

        if is_group:
            child_order = target.get("x-field-order", list(target.get("properties", {}).keys()))
            children = fields_from_schema(target, child_order)
            fields.append(
                {
                    "key": key,
                    "label": prop.get("title", ""),
                    "type": "group",
                    "required": key in schema.get("required", []),
                    "description": prop.get("description", ""),
                    "placeholder": "",
                    "enum": [],
                    "min": None,
                    "max": None,
                    "format": "",
                    "allowed_extensions": [],
                    "is_array": is_array,
                    "items_type": "",
                    "multiline": False,
                    "expand_rows": bool(prop.get("x-expand-rows", False)) if is_array else False,
                    "master_form_id": "",
                    "master_label_key": "",
                    "master_display_fields": [],
                    "children": children,
                }
            )
            continue

        field_type = target.get("type", "string")
        if target.get("format") == "datetime-local":
            field_type = "datetime"
        if target.get("format") == "date":
            field_type = "date"
        if target.get("format") == "time":
            field_type = "time"
        if "enum" in target:
            field_type = "enum"
        if target.get("format") == "binary":
            field_type = "file"
        if target.get("x-field-type") == "master":
            field_type = "master"

        fields.append(
            {
                "key": key,
                "label": prop.get("title", ""),
                "type": field_type,
                "required": key in schema.get("required", []),
                "description": prop.get("description", ""),
                "placeholder": prop.get("x-placeholder", ""),
                "enum": target.get("enum", []),
                "min": target.get("minimum"),
                "max": target.get("maximum"),
                "format": (
                    target.get("format", "")
                    if field_type == "string"
                    else normalize_file_format(target.get("x-file-format"))
                    if field_type == "file"
                    else ""
                ),
                "allowed_extensions": (
                    normalize_allowed_extensions(target.get("x-file-extensions"))
                    if field_type == "file"
                    else []
                ),
                "is_array": is_array,
                "items_type": field_type if is_array else "",
                "multiline": prop.get("x-multiline", False),
                "expand_rows": False,
                "master_form_id": target.get("x-master-form-id", "") if field_type == "master" else "",
                "master_label_key": target.get("x-master-label-key", "") if field_type == "master" else "",
                "master_display_fields": master_display_fields if field_type == "master" else [],
                "children": [],
            }
        )
    return fields
