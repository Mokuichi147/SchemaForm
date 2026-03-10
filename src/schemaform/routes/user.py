from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from schemaform.fields import (
    expand_group_array_rows,
    flatten_filter_fields,
)
from schemaform.filters import (
    apply_filters,
    collect_file_ids,
    resolve_file_names,
)
from schemaform.routes.submissions import (
    build_submission_display_columns,
    build_submission_row_values,
    sort_submissions,
)
from schemaform.schema import fields_from_schema

router = APIRouter()


@router.get("/forms", response_class=HTMLResponse, tags=["user"])
async def list_forms(request: Request) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    forms = storage.forms.list_forms()

    active_forms = [f for f in forms if f.get("status") == "active"]

    sort = request.query_params.get("sort", "name")
    order = request.query_params.get("order", "asc")
    if order not in ("asc", "desc"):
        order = "asc"
    reverse = order == "desc"

    if sort == "updated_at":
        active_forms.sort(
            key=lambda f: str(f.get("updated_at") or ""), reverse=reverse
        )
    else:
        sort = "name"
        active_forms.sort(key=lambda f: (f.get("name") or "").lower(), reverse=reverse)

    return templates.TemplateResponse(
        "forms.html",
        {
            "request": request,
            "forms": active_forms,
            "sort": sort,
            "order": order,
        },
    )


@router.get(
    "/forms/{form_id}/submissions", response_class=HTMLResponse, tags=["user"]
)
async def list_submissions(request: Request, form_id: str) -> HTMLResponse:
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
        expanded_submissions,
        fields,
        dict(request.query_params),
        file_names=file_names,
    )

    display_columns, master_lookup_by_field = build_submission_display_columns(
        storage, fields
    )

    sort = request.query_params.get("sort", "created_at")
    order = request.query_params.get("order", "desc")
    sort_submissions(filtered, sort, order, display_columns, master_lookup_by_field)

    try:
        page = int(request.query_params.get("page", 1))
    except (ValueError, TypeError):
        page = 1
    try:
        page_size = int(request.query_params.get("page_size", 50))
    except (ValueError, TypeError):
        page_size = 50
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
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
        "user_submissions.html",
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
