from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

from schemaform.fields import (
    clean_empty_recursive,
    expand_group_array_rows,
    flatten_fields,
    flatten_filter_fields,
    format_array_group_value,
    get_nested_value,
)
from schemaform.filters import (
    apply_filters,
    collect_file_ids,
    normalize_number,
    parse_bool,
    resolve_file_names,
    value_to_text,
)
from schemaform.master import (
    build_master_reference_context,
    enrich_master_options,
    validate_master_references,
)
from schemaform.schema import fields_from_schema

router = APIRouter()


def admin_guard(request: Request) -> None:
    request.app.state.auth_provider.require_admin(request)


def build_submission_display_columns(
    storage: Any, fields: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, dict[str, dict[str, Any]]]]:
    flat_fields = flatten_fields(fields, expand_rows_for_group_arrays=True)
    display_columns: list[dict[str, Any]] = []
    master_lookup_by_field: dict[str, dict[str, dict[str, Any]]] = {}

    for field in flat_fields:
        flat_key = field["flat_key"]
        if field.get("type") != "master":
            display_columns.append(
                {
                    "kind": "default",
                    "label": field["flat_label"],
                    "field": field,
                }
            )
            continue

        context = build_master_reference_context(storage, field)
        lookup = {row["id"]: row for row in context["records"]}
        for row in context["records"]:
            rid = row["id"]
            if ":" in rid:
                base_id = rid.rsplit(":", 1)[0]
                if base_id not in lookup:
                    lookup[base_id] = row
        master_lookup_by_field[flat_key] = lookup
        display_items = context["display_items"]

        # フォーム参照の選択値そのものは常に列として表示する。
        display_columns.append(
            {
                "kind": "master_label",
                "label": field["flat_label"],
                "field": field,
            }
        )

        if display_items:
            for item in display_items:
                display_columns.append(
                    {
                        "kind": "master_display",
                        "label": f"{field['flat_label']}.{item['label']}",
                        "field": field,
                        "display_key": item["key"],
                    }
                )

    return display_columns, master_lookup_by_field


def render_master_display_text(
    raw_value: Any,
    lookup: dict[str, dict[str, Any]],
    display_key: str | None = None,
) -> str:
    def resolve_one(value: Any) -> str:
        if value in (None, ""):
            return ""
        row = lookup.get(str(value))
        if not row:
            return ""
        if display_key:
            return str((row.get("values") or {}).get(display_key, ""))
        return str(row.get("label", ""))

    if isinstance(raw_value, list):
        parts = [text for text in (resolve_one(item) for item in raw_value) if text]
        return ", ".join(parts)
    return resolve_one(raw_value)


def _get_column_sort_key(
    item: dict[str, Any],
    column: dict[str, Any],
    master_lookup_by_field: dict[str, dict[str, dict[str, Any]]],
) -> tuple[int, Any]:
    """Return a comparable sort key for a single submission row by column."""
    data = item.get("data_json", {})
    field = column["field"]
    flat_key = field["flat_key"]
    value = get_nested_value(data, flat_key)
    is_numeric = field.get("type") in ("number", "integer")

    if value is None or value == "":
        return (1, 0.0) if is_numeric else (1, "")

    if isinstance(value, list):
        if field.get("type") == "master":
            lookup = master_lookup_by_field.get(flat_key, {})
            display_key = column.get("display_key")
            if display_key:
                text = render_master_display_text(value, lookup, str(display_key))
            else:
                text = render_master_display_text(value, lookup)
            return (0, text.lower()) if text else (1, "")
        if is_numeric:
            try:
                return (0, sum(float(v) for v in value))
            except (ValueError, TypeError):
                return (1, 0.0)
        return (0, str(value).lower())

    if isinstance(value, dict):
        return (0, str(value).lower()) if not is_numeric else (1, 0.0)

    if field.get("type") == "master":
        lookup = master_lookup_by_field.get(flat_key, {})
        display_key = column.get("display_key")
        if display_key:
            text = render_master_display_text(value, lookup, str(display_key))
        else:
            text = render_master_display_text(value, lookup)
        return (0, text.lower()) if text else (1, "")

    if is_numeric:
        try:
            return (0, float(value))
        except (ValueError, TypeError):
            return (1, 0.0)

    return (0, str(value).lower())


def sort_submissions(
    submissions: list[dict[str, Any]],
    sort: str,
    order: str,
    display_columns: list[dict[str, Any]],
    master_lookup_by_field: dict[str, dict[str, dict[str, Any]]],
) -> None:
    """Sort the submission list in place."""
    if order not in ("asc", "desc"):
        order = "desc"
    reverse = order == "desc"

    if sort in ("created_at", "updated_at"):
        submissions.sort(
            key=lambda s: str(s.get(sort) or ""), reverse=reverse
        )
        return

    if sort.isdigit():
        col_idx = int(sort)
        if 0 <= col_idx < len(display_columns):
            column = display_columns[col_idx]
            submissions.sort(
                key=lambda item: _get_column_sort_key(
                    item, column, master_lookup_by_field
                ),
                reverse=reverse,
            )
            return

    # Fallback: created_at desc
    submissions.sort(key=lambda s: str(s.get("created_at") or ""), reverse=True)


def build_submission_row_values(
    data: dict[str, Any],
    display_columns: list[dict[str, Any]],
    master_lookup_by_field: dict[str, dict[str, dict[str, Any]]],
    file_names: dict[str, str],
) -> list[str]:
    row_values: list[str] = []
    for column in display_columns:
        field = column["field"]
        flat_key = field["flat_key"]
        value = get_nested_value(data, flat_key)

        if field.get("type") == "group" and field.get("is_array"):
            row_values.append(
                format_array_group_value(value, field.get("children", []))
            )
            continue

        if field.get("type") == "master":
            lookup = master_lookup_by_field.get(flat_key, {})
            if column["kind"] == "master_display":
                row_values.append(
                    render_master_display_text(
                        value, lookup, str(column.get("display_key", ""))
                    )
                )
            else:
                row_values.append(render_master_display_text(value, lookup))
            continue

        row_values.append(value_to_text(value, file_names, field["type"] == "file"))
    return row_values


@router.get(
    "/admin/forms/{form_id}/submissions", response_class=HTMLResponse, tags=["admin"]
)
async def list_submissions(
    request: Request, form_id: str, _: Any = Depends(admin_guard)
) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")

    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    submissions = storage.submissions.list_submissions(form_id)
    expanded_submissions: list[dict[str, Any]] = []
    for submission in submissions:
        data = submission.get("data_json", {})
        for expanded_data in expand_group_array_rows(fields, data):
            expanded_submissions.append({**submission, "data_json": expanded_data})
    file_ids = collect_file_ids(submissions, fields)
    file_names = resolve_file_names(storage.files, file_ids)

    filtered = apply_filters(
        expanded_submissions, fields, dict(request.query_params), file_names=file_names
    )

    display_columns, master_lookup_by_field = build_submission_display_columns(
        storage, fields
    )

    sort = request.query_params.get("sort", "created_at")
    order = request.query_params.get("order", "desc")
    sort_submissions(filtered, sort, order, display_columns, master_lookup_by_field)

    page = int(request.query_params.get("page", 1))
    page_size = int(request.query_params.get("page_size", 50))
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]

    filter_fields = flatten_filter_fields(fields)

    display_rows = []
    for item in page_items:
        data = item.get("data_json", {})
        row_values = build_submission_row_values(
            data,
            display_columns,
            master_lookup_by_field,
            file_names,
        )
        display_rows.append(
            {
                "id": item["id"],
                "created_at": item["created_at"],
                "updated_at": item.get("updated_at"),
                "values": row_values,
            }
        )

    total_pages = max(1, (total + page_size - 1) // page_size)

    return templates.TemplateResponse(
        "submissions.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "display_columns": display_columns,
            "filter_fields": filter_fields,
            "rows": display_rows,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "query": dict(request.query_params),
            "sort": sort,
            "order": order,
        },
    )


@router.post(
    "/admin/forms/{form_id}/submissions/{submission_id}/delete", tags=["admin"]
)
async def delete_submission(
    request: Request, form_id: str, submission_id: str, _: Any = Depends(admin_guard)
) -> RedirectResponse:
    storage = request.app.state.storage
    form = storage.forms.get_form(form_id)
    submission_data = storage.submissions.get_submission(submission_id)

    storage.submissions.delete_submission(submission_id)

    if (
        form
        and submission_data
        and form.get("webhook_url")
        and form.get("webhook_on_delete")
    ):
        from schemaform.webhook import send_webhook

        await send_webhook(form["webhook_url"], "delete", form, submission_data)

    return RedirectResponse(f"/admin/forms/{form_id}/submissions", status_code=303)


@router.get(
    "/admin/forms/{form_id}/submissions/{submission_id}/edit",
    response_class=HTMLResponse,
    tags=["admin"],
)
async def edit_submission(
    request: Request, form_id: str, submission_id: str, _: Any = Depends(admin_guard)
) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    submission = storage.submissions.get_submission(submission_id)
    if not submission or submission["form_id"] != form_id:
        raise HTTPException(status_code=404, detail="送信データが見つかりません")
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    enrich_master_options(storage, fields)
    return templates.TemplateResponse(
        "submission_edit.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "submission": submission,
            "errors": [],
        },
    )


@router.post(
    "/admin/forms/{form_id}/submissions/{submission_id}/edit",
    response_class=HTMLResponse,
    tags=["admin"],
)
async def update_submission(
    request: Request, form_id: str, submission_id: str, _: Any = Depends(admin_guard)
) -> HTMLResponse:
    from jsonschema import Draft7Validator

    from schemaform.routes.public import save_upload
    from schemaform.utils import now_utc

    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    existing = storage.submissions.get_submission(submission_id)
    if not existing or existing["form_id"] != form_id:
        raise HTTPException(status_code=404, detail="送信データが見つかりません")

    form_data = await request.form()
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    enrich_master_options(storage, fields)
    submission: dict[str, Any] = {}
    old_data = existing.get("data_json", {})

    async def collect_fields(
        field_list: list[dict[str, Any]],
        target: dict[str, Any],
        prefix: str,
        old_target: dict[str, Any],
    ) -> None:
        for field in field_list:
            key = field["key"]
            form_key = f"{prefix}{key}" if prefix else key
            field_type = field["type"]
            is_array = field.get("is_array", False)

            if field_type == "group":
                children = field.get("children") or []
                if is_array:
                    indices: set[int] = set()
                    form_prefix = f"{form_key}."
                    for k in form_data:
                        if k.startswith(form_prefix):
                            rest = k[len(form_prefix) :]
                            parts = rest.split(".", 1)
                            if parts[0].isdigit():
                                indices.add(int(parts[0]))
                    items: list[dict[str, Any]] = []
                    old_items = old_target.get(key, []) if isinstance(old_target.get(key), list) else []
                    for order, idx in enumerate(sorted(indices)):
                        item: dict[str, Any] = {}
                        old_item = old_items[order] if order < len(old_items) and isinstance(old_items, list) else {}
                        await collect_fields(children, item, f"{form_key}.{idx}.", old_item)
                        if item:
                            items.append(item)
                    target[key] = items
                else:
                    group_data: dict[str, Any] = {}
                    old_group = old_target.get(key, {}) if isinstance(old_target.get(key), dict) else {}
                    await collect_fields(children, group_data, f"{form_key}.", old_group)
                    target[key] = group_data
                continue

            if is_array:
                if field_type == "file":
                    uploads = form_data.getlist(form_key)
                    file_ids: list[str] = []
                    has_new_upload = any(
                        upload and getattr(upload, "filename", "")
                        for upload in uploads
                    )
                    if has_new_upload:
                        for upload in uploads:
                            if upload and getattr(upload, "filename", ""):
                                file_ids.append(
                                    await save_upload(
                                        upload,
                                        form["id"],
                                        request,
                                        str(field.get("format", "")),
                                        field.get("allowed_extensions") or [],
                                    )
                                )
                        target[key] = file_ids
                    else:
                        target[key] = old_target.get(key, [])
                    continue

                values = [v for v in form_data.getlist(form_key) if v not in (None, "")]
                if field_type in {"number", "integer"}:
                    parsed = [
                        normalize_number(v, field_type == "integer")
                        for v in values
                        if normalize_number(v, field_type == "integer") is not None
                    ]
                    target[key] = parsed
                elif field_type == "boolean":
                    target[key] = [parse_bool(v) for v in values]
                else:
                    target[key] = values
            else:
                if field_type == "file":
                    upload = form_data.get(form_key)
                    if upload and getattr(upload, "filename", ""):
                        target[key] = await save_upload(
                            upload,
                            form["id"],
                            request,
                            str(field.get("format", "")),
                            field.get("allowed_extensions") or [],
                        )
                    else:
                        target[key] = old_target.get(key)
                    continue

                raw_value = form_data.get(form_key)
                if field_type in {"number", "integer"}:
                    target[key] = normalize_number(raw_value, field_type == "integer")
                elif field_type == "boolean":
                    target[key] = parse_bool(raw_value)
                else:
                    target[key] = str(raw_value) if raw_value is not None else None

    await collect_fields(fields, submission, "", old_data)
    submission = clean_empty_recursive(submission) or {}

    validator = Draft7Validator(form["schema_json"])
    errors = sorted(validator.iter_errors(submission), key=lambda err: list(err.path))
    master_errors = validate_master_references(storage, fields, submission)
    if errors or master_errors:
        messages = [f"{error.message}" for error in errors] + master_errors
        return templates.TemplateResponse(
            "submission_edit.html",
            {
                "request": request,
                "form": form,
                "fields": fields,
                "submission": {**existing, "data_json": submission},
                "errors": messages,
            },
        )

    now = now_utc()
    try:
        updated = storage.submissions.update_submission(
            submission_id, {"data_json": submission, "updated_at": now}
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="送信データが見つかりません")

    if (
        form.get("webhook_url")
        and form.get("webhook_on_edit")
    ):
        from schemaform.webhook import send_webhook

        await send_webhook(form["webhook_url"], "edit", form, updated)

    return RedirectResponse(f"/admin/forms/{form_id}/submissions", status_code=303)


@router.get("/admin/forms/{form_id}/export", tags=["admin"])
async def export_submissions(
    request: Request, form_id: str, _: Any = Depends(admin_guard)
) -> PlainTextResponse:
    storage = request.app.state.storage
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")

    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    submissions = storage.submissions.list_submissions(form_id)
    expanded_submissions: list[dict[str, Any]] = []
    for submission in submissions:
        data = submission.get("data_json", {})
        for expanded_data in expand_group_array_rows(fields, data):
            expanded_submissions.append({**submission, "data_json": expanded_data})
    file_ids = collect_file_ids(submissions, fields)
    file_names = resolve_file_names(storage.files, file_ids)
    filtered = apply_filters(
        expanded_submissions, fields, dict(request.query_params), file_names=file_names
    )
    display_columns, master_lookup_by_field = build_submission_display_columns(
        storage, fields
    )

    sort = request.query_params.get("sort", "created_at")
    order = request.query_params.get("order", "desc")
    sort_submissions(filtered, sort, order, display_columns, master_lookup_by_field)

    headers = [column["label"] for column in display_columns]
    rows = [
        build_submission_row_values(
            submission.get("data_json", {}),
            display_columns,
            master_lookup_by_field,
            file_names,
        )
        for submission in filtered
    ]

    fmt = request.query_params.get("format", "csv")
    delimiter = "," if fmt == "csv" else "\t"

    output = io.StringIO()
    writer = csv.writer(output, delimiter=delimiter)
    writer.writerow(headers)
    writer.writerows(rows)

    content_type = "text/csv" if fmt == "csv" else "text/tab-separated-values"
    filename = f"submissions.{fmt}"
    return PlainTextResponse(
        output.getvalue(),
        media_type=content_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/healthz", tags=["system"])
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
