from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from schemaform.master import build_master_display_candidates
from schemaform.schema import (
    fields_from_schema,
    parse_fields_json,
    schema_from_fields,
)
from schemaform.utils import dumps_json, new_short_id, new_ulid, now_utc
from schemaform.webhook import is_valid_webhook_url

router = APIRouter()


async def admin_guard(request: Request) -> None:
    await request.app.state.auth_provider.require_admin(request)


async def form_creator_guard(request: Request) -> None:
    """管理者またはフォーム作成権限グループに所属するユーザーを許可する。"""
    from schemaform.app import can_create_form

    auth = request.app.state.auth_provider
    await auth.require_login(request)
    if not can_create_form(request):
        raise HTTPException(status_code=403, detail="フォーム作成権限がありません")


async def _list_all_groups(request: Request) -> list[dict[str, Any]]:
    """認可済みユーザーで取得できる全グループ一覧（公開先選択用）。"""
    auth = request.app.state.auth_provider
    user = getattr(request.state, "current_user", None)
    if user is None:
        return []
    list_groups = getattr(auth, "list_groups", None)
    if list_groups is None:
        return []
    try:
        groups = await list_groups(user.get("token", ""))
    except Exception:
        return []
    return [{"id": g["id"], "name": g["name"]} for g in groups]


def _parse_publish_group_ids(
    raw_values: list[str], all_group_ids: set[int]
) -> tuple[list[int], list[str]]:
    errors: list[str] = []
    result: list[int] = []
    seen: set[int] = set()
    for raw in raw_values:
        s = str(raw or "").strip()
        if not s:
            continue
        try:
            gid = int(s)
        except ValueError:
            errors.append("公開先グループの指定が不正です")
            continue
        if gid in seen:
            continue
        if all_group_ids and gid not in all_group_ids:
            continue
        seen.add(gid)
        result.append(gid)
    return sorted(result), errors


async def _available_creator_groups(request: Request) -> list[dict[str, Any]]:
    """フォームに紐付け可能なグループ一覧を返す（管理者は全フォーム作成グループ、
    非管理者は自分が所属するフォーム作成グループのみ）。"""
    auth = request.app.state.auth_provider
    storage = request.app.state.storage
    repo = getattr(storage, "settings", None)
    if repo is None:
        return []
    allowed_ids = set(repo.get_form_creator_groups())
    if not allowed_ids:
        return []
    user = getattr(request.state, "current_user", None)
    if user is None:
        return []
    if user.get("is_admin"):
        target_ids = allowed_ids
    else:
        user_group_ids = {g.get("id") for g in (user.get("groups") or [])}
        target_ids = allowed_ids & user_group_ids
    if not target_ids:
        return []
    list_groups = getattr(auth, "list_groups", None)
    if list_groups is None:
        return []
    all_groups = await list_groups(user.get("token", ""))
    return [
        {"id": g["id"], "name": g["name"]}
        for g in all_groups
        if g["id"] in target_ids
    ]


def _ensure_form_editable(request: Request, form: dict | None) -> None:
    from schemaform.app import can_edit_form

    if not can_edit_form(request, form):
        raise HTTPException(status_code=403, detail="このフォームを変更する権限がありません")


def resolve_redirect_target(next_path: Any, default: str = "/admin/forms") -> str:
    candidate = str(next_path or "").strip()
    if not candidate:
        return default
    if not candidate.startswith("/") or candidate.startswith("//"):
        return default
    parsed = urlsplit(candidate)
    if parsed.scheme or parsed.netloc:
        return default
    if not parsed.path.startswith("/admin/forms"):
        return default
    if parsed.query:
        return f"{parsed.path}?{parsed.query}"
    return parsed.path


def build_master_field_catalog(
    storage: Any, current_form_id: str | None = None
) -> tuple[list[dict[str, str]], dict[str, list[dict[str, str]]]]:
    master_forms: list[dict[str, str]] = []
    field_catalog: dict[str, list[dict[str, str]]] = {}
    for form in storage.forms.list_forms():
        form_id = form.get("id")
        if not form_id or (current_form_id and form_id == current_form_id):
            continue
        master_forms.append({"id": form_id, "name": form.get("name") or form_id})
        exclude_form_ids = {current_form_id} if current_form_id else None
        field_catalog[form_id] = build_master_display_candidates(
            storage,
            form_id,
            exclude_form_ids=exclude_form_ids,
        )
    return master_forms, field_catalog


@router.get("/", response_class=HTMLResponse, tags=["admin"])
async def home(request: Request) -> HTMLResponse:
    return RedirectResponse("/forms")


@router.get("/admin/forms", response_class=HTMLResponse, tags=["admin"])
async def list_forms(request: Request, _: Any = Depends(form_creator_guard)) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    forms = storage.forms.list_forms()

    sort = request.query_params.get("sort", "name")
    order = request.query_params.get("order", "asc")
    if order not in ("asc", "desc"):
        order = "asc"
    reverse = order == "desc"

    if sort == "updated_at":
        forms.sort(key=lambda f: str(f.get("updated_at") or ""), reverse=reverse)
    elif sort == "status":
        forms.sort(key=lambda f: f.get("status") or "", reverse=reverse)
    else:
        sort = "name"
        forms.sort(key=lambda f: (f.get("name") or "").lower(), reverse=reverse)

    return templates.TemplateResponse(
        "admin_forms.html",
        {"request": request, "forms": forms, "sort": sort, "order": order},
    )


@router.get("/admin/forms/new", response_class=HTMLResponse, tags=["admin"])
async def new_form(request: Request, _: Any = Depends(form_creator_guard)) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    master_forms, master_field_catalog = build_master_field_catalog(storage)
    available_groups = await _available_creator_groups(request)
    all_groups = await _list_all_groups(request)
    return templates.TemplateResponse(
        "admin_form_builder.html",
        {
            "request": request,
            "form": None,
            "fields": [],
            "fields_json": dumps_json([]),
            "master_forms_json": dumps_json(master_forms),
            "master_field_catalog_json": dumps_json(master_field_catalog),
            "available_groups": available_groups,
            "all_groups": all_groups,
            "errors": [],
        },
    )


@router.post("/admin/forms", response_class=HTMLResponse, tags=["admin"])
async def create_form(request: Request, _: Any = Depends(form_creator_guard)) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form_data = await request.form()
    name = str(form_data.get("name", "")).strip()
    description = str(form_data.get("description", "")).strip()
    fields_json = str(form_data.get("fields_json", ""))
    creator_group_raw = str(form_data.get("creator_group_id", "")).strip()

    fields, errors = parse_fields_json(fields_json)
    master_forms, master_field_catalog = build_master_field_catalog(storage)
    available_groups = await _available_creator_groups(request)
    all_groups = await _list_all_groups(request)
    publish_group_ids, publish_errors = _parse_publish_group_ids(
        form_data.getlist("publish_group_ids"),
        {g["id"] for g in all_groups},
    )
    errors.extend(publish_errors)
    if not name:
        errors.append("フォーム名は必須です")

    user = getattr(request.state, "current_user", None)
    is_admin = bool(user and user.get("is_admin"))
    available_ids = {g["id"] for g in available_groups}
    creator_group_id: int | None = None
    if creator_group_raw:
        try:
            creator_group_id = int(creator_group_raw)
        except ValueError:
            errors.append("作成グループの指定が不正です")
        else:
            if creator_group_id not in available_ids:
                errors.append("選択したグループにフォームを作成する権限がありません")
    if not is_admin and creator_group_id is None:
        if len(available_ids) == 1:
            creator_group_id = next(iter(available_ids))
        else:
            errors.append("作成グループを選択してください")

    def _render(form_extra: dict[str, Any] | None = None) -> HTMLResponse:
        base = {"name": name, "description": description}
        if form_extra:
            base.update(form_extra)
        if creator_group_id is not None:
            base["creator_group_id"] = creator_group_id
        base["publish_group_ids"] = publish_group_ids
        return templates.TemplateResponse(
            "admin_form_builder.html",
            {
                "request": request,
                "form": base,
                "fields": fields,
                "fields_json": dumps_json(fields),
                "master_forms_json": dumps_json(master_forms),
                "master_field_catalog_json": dumps_json(master_field_catalog),
                "available_groups": available_groups,
                "all_groups": all_groups,
                "errors": errors,
            },
        )

    if errors:
        return _render()

    schema, field_order = schema_from_fields(fields)
    form_id = new_ulid()
    public_id = new_short_id()
    now = now_utc()
    webhook_url = str(form_data.get("webhook_url", "")).strip()
    if webhook_url and not is_valid_webhook_url(webhook_url):
        errors.append("Webhook URLはhttp://またはhttps://で始まる有効なURLを指定してください")
        return _render({"webhook_url": webhook_url})
    webhook_on_submit = bool(form_data.get("webhook_on_submit"))
    webhook_on_delete = bool(form_data.get("webhook_on_delete"))
    webhook_on_edit = bool(form_data.get("webhook_on_edit"))
    allow_view_others = bool(form_data.get("allow_view_others"))
    allow_edit_submissions = bool(form_data.get("allow_edit_submissions"))
    storage.forms.create_form(
        {
            "id": form_id,
            "public_id": public_id,
            "name": name,
            "description": description,
            "status": "inactive",
            "schema_json": schema,
            "field_order": field_order,
            "webhook_url": webhook_url,
            "webhook_on_submit": webhook_on_submit,
            "webhook_on_delete": webhook_on_delete,
            "webhook_on_edit": webhook_on_edit,
            "creator_group_id": creator_group_id,
            "publish_group_ids": publish_group_ids,
            "allow_view_others": allow_view_others,
            "allow_edit_submissions": allow_edit_submissions,
            "created_at": now,
            "updated_at": now,
        }
    )
    return RedirectResponse(f"/admin/forms/{form_id}", status_code=303)


@router.get("/admin/forms/{form_id}", response_class=HTMLResponse, tags=["admin"])
async def edit_form(
    request: Request, form_id: str, _: Any = Depends(form_creator_guard)
) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    _ensure_form_editable(request, form)
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    master_forms, master_field_catalog = build_master_field_catalog(
        storage, current_form_id=form_id
    )
    available_groups = await _available_creator_groups(request)
    all_groups = await _list_all_groups(request)
    return templates.TemplateResponse(
        "admin_form_builder.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "fields_json": dumps_json(fields),
            "master_forms_json": dumps_json(master_forms),
            "master_field_catalog_json": dumps_json(master_field_catalog),
            "available_groups": available_groups,
            "all_groups": all_groups,
            "errors": [],
        },
    )


@router.post("/admin/forms/{form_id}", response_class=HTMLResponse, tags=["admin"])
async def update_form(
    request: Request, form_id: str, _: Any = Depends(form_creator_guard)
) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    _ensure_form_editable(request, form)

    form_data = await request.form()
    name = str(form_data.get("name", "")).strip()
    description = str(form_data.get("description", "")).strip()
    fields_json = str(form_data.get("fields_json", ""))

    fields, errors = parse_fields_json(fields_json)
    master_forms, master_field_catalog = build_master_field_catalog(
        storage, current_form_id=form_id
    )
    available_groups = await _available_creator_groups(request)
    all_groups = await _list_all_groups(request)
    publish_group_ids, publish_errors = _parse_publish_group_ids(
        form_data.getlist("publish_group_ids"),
        {g["id"] for g in all_groups},
    )
    errors.extend(publish_errors)
    if not name:
        errors.append("フォーム名は必須です")

    user = getattr(request.state, "current_user", None)
    is_admin = bool(user and user.get("is_admin"))
    creator_group_id: int | None = form.get("creator_group_id")
    if is_admin:
        raw = str(form_data.get("creator_group_id", "")).strip()
        if raw == "":
            creator_group_id = None
        else:
            try:
                new_id = int(raw)
            except ValueError:
                errors.append("作成グループの指定が不正です")
            else:
                allowed_ids = {g["id"] for g in available_groups}
                if new_id not in allowed_ids:
                    errors.append("選択したグループは利用できません")
                else:
                    creator_group_id = new_id

    def _render(form_extra: dict[str, Any] | None = None) -> HTMLResponse:
        base = {**form, "name": name, "description": description}
        base["creator_group_id"] = creator_group_id
        base["publish_group_ids"] = publish_group_ids
        if form_extra:
            base.update(form_extra)
        return templates.TemplateResponse(
            "admin_form_builder.html",
            {
                "request": request,
                "form": base,
                "fields": fields,
                "fields_json": dumps_json(fields),
                "master_forms_json": dumps_json(master_forms),
                "master_field_catalog_json": dumps_json(master_field_catalog),
                "available_groups": available_groups,
                "all_groups": all_groups,
                "errors": errors,
            },
        )

    if errors:
        return _render()

    schema, field_order = schema_from_fields(fields)
    webhook_url = str(form_data.get("webhook_url", "")).strip()
    if webhook_url and not is_valid_webhook_url(webhook_url):
        errors.append("Webhook URLはhttp://またはhttps://で始まる有効なURLを指定してください")
        return _render({"webhook_url": webhook_url})
    webhook_on_submit = bool(form_data.get("webhook_on_submit"))
    webhook_on_delete = bool(form_data.get("webhook_on_delete"))
    webhook_on_edit = bool(form_data.get("webhook_on_edit"))
    allow_view_others = bool(form_data.get("allow_view_others"))
    allow_edit_submissions = bool(form_data.get("allow_edit_submissions"))
    updates = {
        "name": name,
        "description": description,
        "schema_json": schema,
        "field_order": field_order,
        "webhook_url": webhook_url,
        "webhook_on_submit": webhook_on_submit,
        "webhook_on_delete": webhook_on_delete,
        "webhook_on_edit": webhook_on_edit,
        "allow_view_others": allow_view_others,
        "allow_edit_submissions": allow_edit_submissions,
        "publish_group_ids": publish_group_ids,
        "updated_at": now_utc(),
    }
    if is_admin:
        updates["creator_group_id"] = creator_group_id
    updated = storage.forms.update_form(form_id, updates)
    return RedirectResponse(f"/admin/forms/{updated['id']}", status_code=303)


@router.post("/admin/forms/{form_id}/publish", tags=["admin"])
async def publish_form(
    request: Request, form_id: str, _: Any = Depends(form_creator_guard)
) -> RedirectResponse:
    storage = request.app.state.storage
    form = storage.forms.get_form(form_id)
    _ensure_form_editable(request, form)
    storage.forms.set_status(form_id, "active")
    target = resolve_redirect_target(request.query_params.get("next"))
    return RedirectResponse(target, status_code=303)


@router.post("/admin/forms/{form_id}/stop", tags=["admin"])
async def stop_form(
    request: Request, form_id: str, _: Any = Depends(form_creator_guard)
) -> RedirectResponse:
    storage = request.app.state.storage
    form = storage.forms.get_form(form_id)
    _ensure_form_editable(request, form)
    storage.forms.set_status(form_id, "inactive")
    target = resolve_redirect_target(request.query_params.get("next"))
    return RedirectResponse(target, status_code=303)


@router.post("/admin/forms/{form_id}/delete", tags=["admin"])
async def delete_form(
    request: Request, form_id: str, _: Any = Depends(form_creator_guard)
) -> RedirectResponse:
    storage = request.app.state.storage
    form = storage.forms.get_form(form_id)
    _ensure_form_editable(request, form)
    storage.forms.delete_form(form_id)
    return RedirectResponse("/admin/forms", status_code=303)
