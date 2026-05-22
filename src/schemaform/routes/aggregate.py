from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from schemaform.aggregate import aggregate_submissions
from schemaform.routes.submissions import (
    form_editor_guard,
    gather_filtered_submissions,
)

router = APIRouter()


@router.get("/forms/{form_id}/aggregate", response_class=HTMLResponse, tags=["user"])
async def aggregate_view(
    request: Request, form_id: str, _: Any = Depends(form_editor_guard)
) -> HTMLResponse:
    templates = request.app.state.templates
    form, fields, filtered, _file_names = await gather_filtered_submissions(
        request, form_id
    )
    result = aggregate_submissions(fields, filtered)
    return templates.TemplateResponse(
        "aggregate.html",
        {
            "request": request,
            "form": form,
            "query": dict(request.query_params),
            **result,
        },
    )
