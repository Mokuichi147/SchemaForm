from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from schemaform.activity import (
    ACTION_LABELS,
    ACTION_STYLES,
    ACTIVITY_CATEGORY_OPTIONS,
    SUBMISSION_CREATE,
    SUBMISSION_UPDATE,
    category_actions,
)

router = APIRouter()

PAGE_SIZE = 50


async def admin_guard(request: Request) -> None:
    await request.app.state.auth_provider.require_admin(request)


@router.get(
    "/admin/activity",
    response_class=HTMLResponse,
    tags=["admin"],
    response_model=None,
)
async def activity_log(
    request: Request, _: Any = Depends(admin_guard)
) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    repo = getattr(storage, "activities", None)

    category = request.query_params.get("category", "")
    actions = category_actions(category)

    try:
        page = int(request.query_params.get("page", 1))
    except (ValueError, TypeError):
        page = 1
    page = max(1, page)
    offset = (page - 1) * PAGE_SIZE

    if repo is None:
        activities: list[dict[str, Any]] = []
        total = 0
        summary: list[dict[str, Any]] = []
    else:
        activities = repo.list_activities(
            limit=PAGE_SIZE, offset=offset, actions=actions
        )
        total = repo.count_activities(actions=actions)
        summary = repo.form_activity_summary()

    existing_form_ids = {f["id"] for f in storage.forms.list_forms()}

    # 現存する送信のみリンク可能にする（同一ユーザー・同一内容でも個別の送信を特定できるよう、
    # 詳細欄に送信IDの短縮表示とリンクを出す）。
    candidate_ids = {
        a["submission_id"]
        for a in activities
        if a.get("submission_id")
        and a["action"] in (SUBMISSION_CREATE, SUBMISSION_UPDATE)
    }
    existing_submission_ids = {
        sid
        for sid in candidate_ids
        if storage.submissions.get_submission(sid) is not None
    }

    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    return templates.TemplateResponse(
        "admin_activity.html",
        {
            "request": request,
            "activities": activities,
            "summary": summary[:10],
            "existing_form_ids": existing_form_ids,
            "existing_submission_ids": existing_submission_ids,
            "action_labels": ACTION_LABELS,
            "action_styles": ACTION_STYLES,
            "category_options": ACTIVITY_CATEGORY_OPTIONS,
            "category": category,
            "page": page,
            "total": total,
            "total_pages": total_pages,
        },
    )
