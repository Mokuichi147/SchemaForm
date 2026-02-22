from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

from schemaform.fields import (
    expand_group_array_rows,
    flatten_fields,
    flatten_filter_fields,
    format_array_group_value,
    get_nested_value,
)
from schemaform.filters import (
    apply_filters,
    collect_file_ids,
    resolve_file_names,
    value_to_text,
)
from schemaform.master import build_master_reference_context
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

    page = int(request.query_params.get("page", 1))
    page_size = int(request.query_params.get("page_size", 50))
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]

    display_columns, master_lookup_by_field = build_submission_display_columns(
        storage, fields
    )
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
