from __future__ import annotations

from typing import Any

from schemaform.fields import expand_group_array_rows
from schemaform.schema import fields_from_schema
from schemaform.utils import dumps_json, to_iso

_MAX_MASTER_NEST_DEPTH = 6


def _as_non_empty_str(value: Any) -> str:
    text = str(value or "").strip()
    return text


def _get_form(storage: Any, form_id: str, cache: dict[str, Any]) -> dict[str, Any] | None:
    forms_cache = cache.setdefault("forms", {})
    if form_id not in forms_cache:
        forms_cache[form_id] = storage.forms.get_form(form_id)
    form = forms_cache.get(form_id)
    return form if isinstance(form, dict) else None


def _get_form_fields(storage: Any, form_id: str, cache: dict[str, Any]) -> list[dict[str, Any]]:
    fields_cache = cache.setdefault("form_fields", {})
    if form_id in fields_cache:
        cached = fields_cache[form_id]
        return cached if isinstance(cached, list) else []

    form = _get_form(storage, form_id, cache)
    if not form:
        fields_cache[form_id] = []
        return []

    fields = fields_from_schema(form.get("schema_json", {}), form.get("field_order", []))
    fields_cache[form_id] = fields
    return fields


def _get_submission_map(storage: Any, form_id: str, cache: dict[str, Any]) -> dict[str, dict[str, Any]]:
    submissions_cache = cache.setdefault("submission_map", {})
    if form_id in submissions_cache:
        cached = submissions_cache[form_id]
        return cached if isinstance(cached, dict) else {}

    rows: dict[str, dict[str, Any]] = {}
    for submission in storage.submissions.list_submissions(form_id):
        submission_id = _as_non_empty_str(submission.get("id"))
        if not submission_id:
            continue
        rows[submission_id] = submission
    submissions_cache[form_id] = rows
    return rows


def _get_submission_by_id(
    storage: Any,
    form_id: str,
    submission_id: str,
    cache: dict[str, Any],
) -> dict[str, Any] | None:
    rows = _get_submission_map(storage, form_id, cache)
    row = rows.get(submission_id)
    if row is not None:
        return row if isinstance(row, dict) else None

    if ":" in submission_id:
        base_id, idx_str = submission_id.rsplit(":", 1)
        try:
            row_index = int(idx_str)
        except ValueError:
            return None
        row = rows.get(base_id)
        if not isinstance(row, dict):
            return None
        source_fields = _get_form_fields(storage, form_id, cache)
        data = row.get("data_json", {})
        if isinstance(data, dict) and source_fields:
            expanded_cache = cache.setdefault("expanded_rows", {})
            exp_key = (form_id, base_id)
            if exp_key not in expanded_cache:
                expanded_cache[exp_key] = expand_group_array_rows(source_fields, data)
            expanded = expanded_cache[exp_key]
            if row_index < len(expanded):
                return {**row, "data_json": expanded[row_index]}
        return None

    return None


def _get_field_map(fields: list[dict[str, Any]], cache: dict[str, Any]) -> dict[str, dict[str, Any]]:
    map_cache = cache.setdefault("field_map", {})
    cache_key = id(fields)
    if cache_key in map_cache:
        cached = map_cache[cache_key]
        return cached if isinstance(cached, dict) else {}

    mapping: dict[str, dict[str, Any]] = {}
    for field in fields:
        key = _as_non_empty_str(field.get("key"))
        if not key:
            continue
        mapping[key] = field
    map_cache[cache_key] = mapping
    return mapping


def _flatten_values(value: Any) -> list[Any]:
    if isinstance(value, list):
        result: list[Any] = []
        for item in value:
            result.extend(_flatten_values(item))
        return result
    return [value]


def _resolve_path_values(
    storage: Any,
    data: dict[str, Any],
    fields: list[dict[str, Any]],
    dotted_key: str,
    cache: dict[str, Any],
    visited_forms: set[str] | None = None,
) -> list[Any]:
    parts = [part for part in dotted_key.split(".") if part]
    if not parts or not isinstance(data, dict):
        return []

    contexts: list[tuple[Any, list[dict[str, Any]], set[str]]] = [
        (data, fields, set(visited_forms or set())),
    ]

    for index, part in enumerate(parts):
        terminal = index == len(parts) - 1
        next_contexts: list[tuple[Any, list[dict[str, Any]], set[str]]] = []

        for current_value, current_fields, current_visited in contexts:
            next_contexts.extend(
                _resolve_single_part(
                    storage=storage,
                    value=current_value,
                    fields=current_fields,
                    key_part=part,
                    terminal=terminal,
                    cache=cache,
                    visited_forms=current_visited,
                )
            )

        contexts = next_contexts
        if not contexts:
            return []

    results: list[Any] = []
    for resolved_value, _, _ in contexts:
        results.extend(_flatten_values(resolved_value))
    return results


def _resolve_single_part(
    storage: Any,
    value: Any,
    fields: list[dict[str, Any]],
    key_part: str,
    terminal: bool,
    cache: dict[str, Any],
    visited_forms: set[str],
) -> list[tuple[Any, list[dict[str, Any]], set[str]]]:
    if isinstance(value, list):
        contexts: list[tuple[Any, list[dict[str, Any]], set[str]]] = []
        for item in value:
            contexts.extend(
                _resolve_single_part(
                    storage=storage,
                    value=item,
                    fields=fields,
                    key_part=key_part,
                    terminal=terminal,
                    cache=cache,
                    visited_forms=visited_forms,
                )
            )
        return contexts

    if not isinstance(value, dict):
        return []

    field = _get_field_map(fields, cache).get(key_part)
    if not field:
        return []

    raw_child = value.get(key_part)
    field_type = _as_non_empty_str(field.get("type"))

    if field_type == "group":
        children = field.get("children") or []
        if field.get("is_array"):
            if isinstance(raw_child, list):
                contexts: list[tuple[Any, list[dict[str, Any]], set[str]]] = []
                for item in raw_child:
                    contexts.append((item, children, set(visited_forms)))
                return contexts
            if isinstance(raw_child, dict):
                return [(raw_child, children, set(visited_forms))]
            return []
        return [(raw_child, children, set(visited_forms))]

    if field_type == "master":
        source_form_id = _as_non_empty_str(field.get("master_form_id"))
        if not source_form_id:
            return []

        values = raw_child if isinstance(raw_child, list) else [raw_child]
        contexts: list[tuple[Any, list[dict[str, Any]], set[str]]] = []
        for raw_id in values:
            submission_id = _as_non_empty_str(raw_id)
            if not submission_id:
                continue
            submission = _get_submission_by_id(storage, source_form_id, submission_id, cache)
            if not submission:
                continue

            if source_form_id in visited_forms:
                continue
            next_visited = visited_forms | {source_form_id}

            if terminal:
                nested_candidates = _get_form_candidates(
                    storage=storage,
                    source_form_id=source_form_id,
                    cache=cache,
                    exclude_form_ids=visited_forms,
                )
                nested_fallback_keys = _fallback_keys_from_candidates(nested_candidates)
                label_text = _build_submission_label(
                    storage=storage,
                    source_form_id=source_form_id,
                    submission=submission,
                    label_key=_as_non_empty_str(field.get("master_label_key")),
                    fallback_keys=nested_fallback_keys,
                    cache=cache,
                    visited_forms=next_visited,
                )
                contexts.append((label_text, [], set(visited_forms)))
                continue

            next_fields = _get_form_fields(storage, source_form_id, cache)
            next_data = submission.get("data_json", {})
            if not isinstance(next_data, dict):
                continue
            contexts.append((next_data, next_fields, next_visited))
        return contexts

    return [(raw_child, fields, set(visited_forms))]


def master_label_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return dumps_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _label_from_key(
    storage: Any,
    data: dict[str, Any],
    fields: list[dict[str, Any]],
    dotted_key: str,
    cache: dict[str, Any],
    visited_forms: set[str] | None = None,
) -> str:
    if not dotted_key:
        return ""

    values = _resolve_path_values(
        storage=storage,
        data=data,
        fields=fields,
        dotted_key=dotted_key,
        cache=cache,
        visited_forms=visited_forms,
    )
    labels: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = master_label_text(value).strip()
        if not text or text in seen:
            continue
        labels.append(text)
        seen.add(text)
    return ", ".join(labels)


def _collect_candidate_fields(
    storage: Any,
    fields: list[dict[str, Any]],
    cache: dict[str, Any],
    *,
    prefix_key: str = "",
    prefix_label: str = "",
    visited_forms: set[str],
    depth: int,
    seen_keys: set[str],
) -> list[dict[str, str]]:
    if depth > _MAX_MASTER_NEST_DEPTH:
        return []

    candidates: list[dict[str, str]] = []
    for field in fields:
        key = _as_non_empty_str(field.get("key"))
        if not key:
            continue
        label = _as_non_empty_str(field.get("label")) or key
        full_key = f"{prefix_key}.{key}" if prefix_key else key
        full_label = f"{prefix_label}.{label}" if prefix_label else label
        field_type = _as_non_empty_str(field.get("type"))

        if field_type == "group":
            candidates.extend(
                _collect_candidate_fields(
                    storage=storage,
                    fields=field.get("children") or [],
                    cache=cache,
                    prefix_key=full_key,
                    prefix_label=full_label,
                    visited_forms=set(visited_forms),
                    depth=depth,
                    seen_keys=seen_keys,
                )
            )
            continue

        if field_type != "file" and full_key not in seen_keys:
            candidates.append(
                {
                    "key": full_key,
                    "label": full_label,
                    "type": field_type,
                }
            )
            seen_keys.add(full_key)

        if field_type != "master":
            continue

        source_form_id = _as_non_empty_str(field.get("master_form_id"))
        if not source_form_id or source_form_id in visited_forms:
            continue
        nested_fields = _get_form_fields(storage, source_form_id, cache)
        if not nested_fields:
            continue

        candidates.extend(
            _collect_candidate_fields(
                storage=storage,
                fields=nested_fields,
                cache=cache,
                prefix_key=full_key,
                prefix_label=full_label,
                visited_forms=visited_forms | {source_form_id},
                depth=depth + 1,
                seen_keys=seen_keys,
            )
        )

    return candidates


def _get_form_candidates(
    storage: Any,
    source_form_id: str,
    cache: dict[str, Any],
    *,
    exclude_form_ids: set[str] | None = None,
) -> list[dict[str, str]]:
    cache_key = (
        source_form_id,
        tuple(sorted([item for item in (exclude_form_ids or set()) if item])),
    )
    candidates_cache = cache.setdefault("candidate_map", {})
    if cache_key in candidates_cache:
        cached = candidates_cache[cache_key]
        return cached if isinstance(cached, list) else []

    base_fields = _get_form_fields(storage, source_form_id, cache)
    if not base_fields:
        candidates_cache[cache_key] = []
        return []

    visited_forms = {source_form_id, *(exclude_form_ids or set())}
    candidates = _collect_candidate_fields(
        storage=storage,
        fields=base_fields,
        cache=cache,
        prefix_key="",
        prefix_label="",
        visited_forms=visited_forms,
        depth=0,
        seen_keys=set(),
    )
    candidates_cache[cache_key] = candidates
    return candidates


def _fallback_keys_from_candidates(candidates: list[dict[str, str]]) -> list[str]:
    return [
        _as_non_empty_str(candidate.get("key"))
        for candidate in candidates
        if _as_non_empty_str(candidate.get("key"))
    ]


def _build_submission_label(
    storage: Any,
    source_form_id: str,
    submission: dict[str, Any],
    label_key: str,
    cache: dict[str, Any],
    visited_forms: set[str] | None = None,
    fallback_keys: list[str] | None = None,
    fallback_index: int | None = None,
) -> str:
    return build_master_option_label(
        storage=storage,
        source_form_id=source_form_id,
        submission=submission,
        label_key=label_key,
        fallback_keys=fallback_keys,
        fallback_index=fallback_index,
        cache=cache,
        visited_forms=visited_forms,
    )


def build_master_option_label(
    storage: Any,
    source_form_id: str,
    submission: dict[str, Any],
    label_key: str,
    fallback_keys: list[str] | None = None,
    fallback_index: int | None = None,
    cache: dict[str, Any] | None = None,
    visited_forms: set[str] | None = None,
) -> str:
    if cache is None:
        cache = {}
    data = submission.get("data_json", {})
    if isinstance(data, dict):
        source_fields = _get_form_fields(storage, source_form_id, cache)
        if label_key:
            label_text = _label_from_key(
                storage=storage,
                data=data,
                fields=source_fields,
                dotted_key=label_key,
                cache=cache,
                visited_forms=visited_forms,
            )
            if label_text:
                return label_text

        for key in fallback_keys or []:
            label_text = _label_from_key(
                storage=storage,
                data=data,
                fields=source_fields,
                dotted_key=key,
                cache=cache,
                visited_forms=visited_forms,
            )
            if label_text:
                return label_text

    created_at = submission.get("created_at")
    created_text = to_iso(created_at).replace("T", " ")[:16] if created_at else ""
    if created_text:
        return created_text
    if fallback_index is not None:
        return f"送信データ {fallback_index}"
    return "送信データ"


def build_master_display_values(
    storage: Any,
    source_form_id: str,
    submission: dict[str, Any],
    display_keys: list[str],
    cache: dict[str, Any] | None = None,
    visited_forms: set[str] | None = None,
) -> dict[str, str]:
    if cache is None:
        cache = {}
    data = submission.get("data_json", {})
    if not isinstance(data, dict):
        return {}
    source_fields = _get_form_fields(storage, source_form_id, cache)
    values: dict[str, str] = {}
    for key in display_keys:
        dotted_key = _as_non_empty_str(key)
        if not dotted_key:
            continue
        value_text = _label_from_key(
            storage=storage,
            data=data,
            fields=source_fields,
            dotted_key=dotted_key,
            cache=cache,
            visited_forms=visited_forms,
        )
        if not value_text:
            continue
        values[dotted_key] = value_text
    return values


def build_master_display_candidates(
    storage: Any,
    source_form_id: str,
    *,
    exclude_form_ids: set[str] | None = None,
) -> list[dict[str, str]]:
    source_id = _as_non_empty_str(source_form_id)
    if not source_id:
        return []
    cache: dict[str, Any] = {}
    candidates = _get_form_candidates(
        storage=storage,
        source_form_id=source_id,
        cache=cache,
        exclude_form_ids=exclude_form_ids,
    )
    return [{"key": item["key"], "label": item["label"]} for item in candidates]


def _has_expand_rows(fields: list[dict[str, Any]]) -> bool:
    for field in fields:
        if field.get("type") == "group":
            if field.get("is_array") and field.get("expand_rows"):
                return True
            if _has_expand_rows(field.get("children") or []):
                return True
    return False


def build_master_reference_context(storage: Any, field: dict[str, Any]) -> dict[str, Any]:
    source_form_id = _as_non_empty_str(field.get("master_form_id"))
    label_key = _as_non_empty_str(field.get("master_label_key"))
    selected_display_keys = [
        _as_non_empty_str(item)
        for item in (field.get("master_display_fields") or [])
        if _as_non_empty_str(item)
    ]
    cache: dict[str, Any] = {}

    candidates = _get_form_candidates(storage, source_form_id, cache) if source_form_id else []
    label_by_key = {item["key"]: item["label"] for item in candidates if item.get("key")}
    available_keys = set(label_by_key.keys())
    fallback_keys = _fallback_keys_from_candidates(candidates)

    effective_label_key = label_key if label_key in available_keys else ""
    effective_display_keys = [key for key in selected_display_keys if key in available_keys]
    display_items = [
        {
            "key": key,
            "label": label_by_key.get(key, key),
        }
        for key in effective_display_keys
    ]

    records: list[dict[str, Any]] = []
    if source_form_id:
        source_fields = _get_form_fields(storage, source_form_id, cache)
        use_expansion = _has_expand_rows(source_fields) if source_fields else False
        submissions = storage.submissions.list_submissions(source_form_id)
        record_index = 0

        def _append_record(record_id: str, sub: dict[str, Any]) -> None:
            nonlocal record_index
            record_index += 1
            records.append(
                {
                    "id": record_id,
                    "label": build_master_option_label(
                        storage=storage,
                        source_form_id=source_form_id,
                        submission=sub,
                        label_key=effective_label_key,
                        fallback_keys=fallback_keys,
                        fallback_index=record_index,
                        cache=cache,
                        visited_forms={source_form_id},
                    ),
                    "values": build_master_display_values(
                        storage=storage,
                        source_form_id=source_form_id,
                        submission=sub,
                        display_keys=effective_display_keys,
                        cache=cache,
                        visited_forms={source_form_id},
                    ),
                }
            )

        for submission in submissions:
            submission_id = _as_non_empty_str(submission.get("id"))
            if not submission_id:
                continue

            if use_expansion:
                data = submission.get("data_json", {})
                expanded_rows = (
                    expand_group_array_rows(source_fields, data)
                    if isinstance(data, dict)
                    else [{}]
                )
                for row_idx, expanded_data in enumerate(expanded_rows):
                    _append_record(
                        f"{submission_id}:{row_idx}",
                        {**submission, "data_json": expanded_data},
                    )
            else:
                _append_record(submission_id, submission)

    return {
        "source_form_id": source_form_id,
        "label_key": effective_label_key,
        "display_keys": effective_display_keys,
        "display_items": display_items,
        "records": records,
    }


def enrich_master_options(storage: Any, fields: list[dict[str, Any]]) -> None:
    for field in fields:
        if field.get("type") == "group":
            enrich_master_options(storage, field.get("children") or [])
            continue
        if field.get("type") != "master":
            continue

        context = build_master_reference_context(storage, field)
        field["master_display_fields"] = context["display_keys"]
        field["master_display_items"] = context["display_items"]
        options: list[dict[str, Any]] = []
        for record in context["records"]:
            options.append(
                {
                    "value": record["id"],
                    "label": record["label"],
                    "display_json": dumps_json(record["values"]),
                }
            )
        field["master_options"] = options


def _extract_base_id(value: Any) -> str:
    text = str(value)
    if ":" in text:
        return text.rsplit(":", 1)[0]
    return text


def validate_master_references(storage: Any, fields: list[dict[str, Any]], data: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    id_cache: dict[str, set[str]] = {}

    def valid_ids(form_id: str) -> set[str]:
        if form_id not in id_cache:
            id_cache[form_id] = {
                str(item.get("id", ""))
                for item in storage.submissions.list_submissions(form_id)
                if item.get("id")
            }
        return id_cache[form_id]

    def validate(field_list: list[dict[str, Any]], target: dict[str, Any]) -> None:
        if not isinstance(target, dict):
            return
        for field in field_list:
            key = _as_non_empty_str(field.get("key"))
            if not key:
                continue
            value = target.get(key)
            if field.get("type") == "group":
                children = field.get("children") or []
                if field.get("is_array"):
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                validate(children, item)
                elif isinstance(value, dict):
                    validate(children, value)
                continue

            if field.get("type") != "master":
                continue

            source_form_id = _as_non_empty_str(field.get("master_form_id"))
            if not source_form_id:
                continue
            master_ids = valid_ids(source_form_id)
            label = field.get("label") or key

            if field.get("is_array"):
                if not isinstance(value, list):
                    continue
                invalid = [
                    item
                    for item in value
                    if item not in (None, "") and _extract_base_id(item) not in master_ids
                ]
                if invalid:
                    errors.append(f"{label}: 選択値に無効な項目があります")
            else:
                if value in (None, ""):
                    continue
                if _extract_base_id(value) not in master_ids:
                    errors.append(f"{label}: 選択値が不正です")

    validate(fields, data)
    return errors
