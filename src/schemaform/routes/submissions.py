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
    csv_headers_and_rows,
    resolve_file_names,
    value_to_text,
)
from schemaform.schema import fields_from_schema

router = APIRouter()


def admin_guard(request: Request) -> None:
    request.app.state.auth_provider.require_admin(request)


@router.get("/admin/forms/{form_id}/submissions", response_class=HTMLResponse, tags=["admin"])
async def list_submissions(request: Request, form_id: str, _: Any = Depends(admin_guard)) -> HTMLResponse:
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

    flat_fields = flatten_fields(fields, expand_rows_for_group_arrays=True)
    filter_fields = flatten_filter_fields(fields)

    display_rows = []
    for item in page_items:
        row_values = []
        data = item.get("data_json", {})
        for ff in flat_fields:
            fk = ff["flat_key"]
            value = get_nested_value(data, fk)
            if ff.get("type") == "group" and ff.get("is_array"):
                row_values.append(format_array_group_value(value, ff.get("children", [])))
            else:
                row_values.append(
                    value_to_text(value, file_names, ff["type"] == "file")
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
            "flat_fields": flat_fields,
            "filter_fields": filter_fields,
            "rows": display_rows,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "query": dict(request.query_params),
        },
    )


@router.post("/admin/forms/{form_id}/submissions/{submission_id}/delete", tags=["admin"])
async def delete_submission(
    request: Request, form_id: str, submission_id: str, _: Any = Depends(admin_guard)
) -> RedirectResponse:
    storage = request.app.state.storage
    storage.submissions.delete_submission(submission_id)
    return RedirectResponse(f"/admin/forms/{form_id}/submissions", status_code=303)


@router.get("/admin/forms/{form_id}/export", tags=["admin"])
async def export_submissions(request: Request, form_id: str, _: Any = Depends(admin_guard)) -> PlainTextResponse:
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

    headers, rows = csv_headers_and_rows(fields, filtered, file_names)

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
