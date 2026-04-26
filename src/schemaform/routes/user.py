from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from schemaform.fields import (
    expand_group_array_rows,
    flatten_filter_fields,
)
from schemaform.filters import (
    apply_filters,
    collect_file_ids,
    resolve_file_infos,
)
from schemaform.routes.submissions import (
    build_submission_display_columns,
    build_submission_raw_values,
    build_submission_row_values,
    collect_submission_master_display_file_ids,
    sort_submissions,
)
from schemaform.schema import fields_from_schema

router = APIRouter()


def _check_submission_owner(
    submission: dict[str, Any], current_user: dict[str, Any] | None
) -> None:
    """本人の送信でなければ 403。管理者は許可。"""
    if current_user is None:
        raise HTTPException(status_code=401, detail="ログインが必要です")
    if current_user.get("is_admin"):
        return
    owner_id = submission.get("user_id")
    if owner_id is None or owner_id != current_user.get("id"):
        raise HTTPException(status_code=403, detail="この送信は編集できません")


@router.get("/forms", tags=["user"], response_model=None)
async def list_forms(request: Request) -> HTMLResponse | RedirectResponse:
    from schemaform.app import can_create_form

    current_user = getattr(request.state, "current_user", None)
    if current_user and (
        current_user.get("is_admin") or can_create_form(request)
    ):
        return RedirectResponse("/admin/forms", status_code=303)
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

    display_columns, master_lookup_by_field = build_submission_display_columns(
        storage, fields
    )
    file_ids |= collect_submission_master_display_file_ids(
        submissions, display_columns, master_lookup_by_field
    )
    file_infos = resolve_file_infos(storage.files, file_ids)
    file_names = {fid: info["name"] for fid, info in file_infos.items()}

    filtered = apply_filters(
        expanded_submissions,
        fields,
        dict(request.query_params),
        file_names=file_names,
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
        raw_values = build_submission_raw_values(
            data, display_columns, master_lookup_by_field
        )
        display_rows.append(
            {
                "id": item["id"],
                "created_at": item["created_at"],
                "updated_at": item.get("updated_at"),
                "user_id": item.get("user_id"),
                "username": item.get("username"),
                "values": row_values,
                "raw_values": raw_values,
            }
        )

    total_pages = max(1, (total + page_size - 1) // page_size)

    current_user = getattr(request.state, "current_user", None)
    for row in display_rows:
        row["editable"] = (
            current_user is not None
            and row.get("user_id") is not None
            and (
                current_user.get("is_admin")
                or row["user_id"] == current_user.get("id")
            )
        )

    return templates.TemplateResponse(
        "user_submissions.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "display_columns": display_columns,
            "filter_fields": filter_fields,
            "rows": display_rows,
            "file_infos": file_infos,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "query": dict(request.query_params),
            "sort": sort,
            "order": order,
        },
    )


@router.get(
    "/forms/{form_id}/submissions/{submission_id}/edit",
    response_class=HTMLResponse,
    tags=["user"],
)
async def edit_submission(
    request: Request, form_id: str, submission_id: str
) -> HTMLResponse:
    from schemaform.master import enrich_master_options

    storage = request.app.state.storage
    templates = request.app.state.templates
    current_user = getattr(request.state, "current_user", None)

    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    submission = storage.submissions.get_submission(submission_id)
    if not submission or submission["form_id"] != form_id:
        raise HTTPException(status_code=404, detail="送信データが見つかりません")

    _check_submission_owner(submission, current_user)

    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    enrich_master_options(storage, fields)
    file_ids = collect_file_ids([submission], fields)
    file_infos = resolve_file_infos(storage.files, file_ids)
    return templates.TemplateResponse(
        "submission_edit.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "submission": submission,
            "file_infos": file_infos,
            "errors": [],
            "cancel_url": f"/forms/{form_id}/submissions",
            "action_url": f"/forms/{form_id}/submissions/{submission_id}/edit",
        },
    )


@router.post(
    "/forms/{form_id}/submissions/{submission_id}/edit",
    response_class=HTMLResponse,
    tags=["user"],
    response_model=None,
)
async def update_submission(
    request: Request, form_id: str, submission_id: str
) -> HTMLResponse | RedirectResponse:
    from schemaform.routes.submissions import update_submission as admin_update

    storage = request.app.state.storage
    current_user = getattr(request.state, "current_user", None)
    existing = storage.submissions.get_submission(submission_id)
    if not existing or existing.get("form_id") != form_id:
        raise HTTPException(status_code=404, detail="送信データが見つかりません")
    _check_submission_owner(existing, current_user)

    response = await admin_update(request, form_id, submission_id, None)
    if isinstance(response, RedirectResponse):
        return RedirectResponse(f"/forms/{form_id}/submissions", status_code=303)
    return response


@router.post(
    "/forms/{form_id}/submissions/{submission_id}/delete", tags=["user"]
)
async def delete_submission(
    request: Request, form_id: str, submission_id: str
) -> RedirectResponse:
    storage = request.app.state.storage
    current_user = getattr(request.state, "current_user", None)
    form = storage.forms.get_form(form_id)
    submission = storage.submissions.get_submission(submission_id)
    if not submission or submission.get("form_id") != form_id:
        raise HTTPException(status_code=404, detail="送信データが見つかりません")
    _check_submission_owner(submission, current_user)

    storage.submissions.delete_submission(submission_id)

    if (
        form
        and submission
        and form.get("webhook_url")
        and form.get("webhook_on_delete")
    ):
        from schemaform.webhook import send_webhook

        await send_webhook(form["webhook_url"], "delete", form, submission)

    return RedirectResponse(f"/forms/{form_id}/submissions", status_code=303)
