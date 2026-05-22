from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from schemaform.aggregate import NUMERIC_TYPES, aggregate_submissions
from schemaform.fields import flatten_fields, get_nested_value
from schemaform.routes.submissions import (
    build_full_table_context,
    form_editor_guard,
    gather_filtered_submissions,
)

router = APIRouter()

DRILL_TYPES = NUMERIC_TYPES | {"enum", "boolean"}


@router.get("/forms/{form_id}/aggregate", response_class=HTMLResponse, tags=["user"])
async def aggregate_view(
    request: Request, form_id: str, _: Any = Depends(form_editor_guard)
) -> HTMLResponse:
    templates = request.app.state.templates
    form, fields, filtered, _file_names = await gather_filtered_submissions(
        request, form_id
    )
    result = aggregate_submissions(fields, filtered)
    table = await build_full_table_context(request, fields, filtered)

    drill_keys = [
        field["flat_key"]
        for field in flatten_fields(fields, expand_rows_for_group_arrays=True)
        if field.get("type") in DRILL_TYPES
    ]
    for row, item in zip(table["rows"], filtered):
        data = item.get("data_json", {})
        drill: dict[str, Any] = {}
        for key in drill_keys:
            value = get_nested_value(data, key)
            if value is not None and value != "":
                drill[key] = value
        row["drill"] = drill
        created = item.get("created_at")
        row["date"] = (
            created.astimezone().date().isoformat()
            if isinstance(created, datetime)
            else ""
        )

    return templates.TemplateResponse(
        "aggregate.html",
        {
            "request": request,
            "form": form,
            "query": dict(request.query_params),
            **result,
            **table,
        },
    )
