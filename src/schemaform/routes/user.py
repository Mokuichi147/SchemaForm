from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from schemaform.filters import collect_file_ids, resolve_file_infos
from schemaform.routes.submissions import (
    build_submission_list_context,
    perform_update_submission,
)
from schemaform.schema import fields_from_schema

router = APIRouter()


def _check_submission_owner(
    request: Any,
    form: dict[str, Any] | None,
    submission: dict[str, Any],
    current_user: dict[str, Any] | None,
) -> None:
    """本人の送信、またはフォームを管理できるユーザーでなければ 403。"""
    from schemaform.app import can_edit_form

    if current_user is None:
        raise HTTPException(status_code=401, detail="ログインが必要です")
    if can_edit_form(request, form):
        return
    owner_id = submission.get("user_id")
    if owner_id is None or owner_id != current_user.get("id"):
        raise HTTPException(status_code=403, detail="この送信は編集できません")


def _check_submission_editable(
    request: Any, form: dict[str, Any], current_user: dict[str, Any] | None
) -> None:
    """フォームの設定で送信内容変更が無効化されている場合は 403。
    フォームを管理できるユーザーは許可。"""
    from schemaform.app import can_edit_form

    if can_edit_form(request, form):
        return
    if form.get("disallow_edit_submissions", False):
        raise HTTPException(
            status_code=403, detail="このフォームでは送信内容の変更は許可されていません"
        )


@router.get("/forms", tags=["user"], response_model=None)
async def list_forms(request: Request) -> HTMLResponse:
    from schemaform.app import can_edit_form, can_input_form

    storage = request.app.state.storage
    templates = request.app.state.templates
    forms = storage.forms.list_forms()

    current_user = getattr(request.state, "current_user", None)
    is_admin = bool(current_user and current_user.get("is_admin"))

    if is_admin:
        visible_forms = list(forms)
    else:
        visible_forms = [
            f
            for f in forms
            if (
                (f.get("status") == "active" and can_input_form(request, f))
                or can_edit_form(request, f)
            )
        ]

    sort = request.query_params.get("sort", "name")
    order = request.query_params.get("order", "asc")
    if order not in ("asc", "desc"):
        order = "asc"
    reverse = order == "desc"

    if sort == "updated_at":
        visible_forms.sort(
            key=lambda f: str(f.get("updated_at") or ""), reverse=reverse
        )
    elif sort == "status" and is_admin:
        visible_forms.sort(key=lambda f: f.get("status") or "", reverse=reverse)
    else:
        sort = "name"
        visible_forms.sort(
            key=lambda f: (f.get("name") or "").lower(), reverse=reverse
        )

    template_name = "admin_forms.html" if is_admin else "forms.html"
    return templates.TemplateResponse(
        template_name,
        {
            "request": request,
            "forms": visible_forms,
            "sort": sort,
            "order": order,
        },
    )


@router.get(
    "/forms/{form_id}/submissions", response_class=HTMLResponse, tags=["user"]
)
async def list_submissions(request: Request, form_id: str) -> HTMLResponse:
    from schemaform.app import can_edit_form, can_view_form

    storage = request.app.state.storage
    templates = request.app.state.templates

    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")

    current_user = getattr(request.state, "current_user", None)
    publish_ids = form.get("publish_group_ids") or []
    if publish_ids and current_user is None:
        await request.app.state.auth_provider.require_login(request)
    if not can_view_form(request, form):
        raise HTTPException(status_code=403, detail="このフォームを閲覧する権限がありません")
    is_manager_view = can_edit_form(request, form)
    if not form.get("allow_view_others", True) and not is_manager_view:
        await request.app.state.auth_provider.require_login(request)

    filter_user_id: int | None = None
    if not is_manager_view and not form.get("allow_view_others", True):
        filter_user_id = current_user.get("id") if current_user else None

    context = await build_submission_list_context(
        request,
        form_id,
        include_user_display_map=is_manager_view,
        filter_user_id=filter_user_id,
    )

    if is_manager_view:
        template_name = "submissions.html"
    else:
        template_name = "user_submissions.html"
        allow_edit = not form.get("disallow_edit_submissions", False)
        for row in context["rows"]:
            owner_id = row.get("user_id")
            if current_user is None:
                row["editable"] = False
            elif not allow_edit:
                row["editable"] = False
            else:
                row["editable"] = (
                    owner_id is not None and owner_id == current_user.get("id")
                )

    return templates.TemplateResponse(
        template_name,
        {"request": request, **context},
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

    _check_submission_owner(request, form, submission, current_user)
    _check_submission_editable(request, form, current_user)

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
    storage = request.app.state.storage
    current_user = getattr(request.state, "current_user", None)
    existing = storage.submissions.get_submission(submission_id)
    if not existing or existing.get("form_id") != form_id:
        raise HTTPException(status_code=404, detail="送信データが見つかりません")
    form = storage.forms.get_form(form_id)
    _check_submission_owner(request, form, existing, current_user)
    if form is not None:
        _check_submission_editable(request, form, current_user)

    return await perform_update_submission(request, form_id, submission_id)


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
    _check_submission_owner(request, form, submission, current_user)
    if form is not None:
        _check_submission_editable(request, form, current_user)

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
