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


def admin_guard(request: Request) -> None:
    request.app.state.auth_provider.require_admin(request)


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
    return RedirectResponse("/admin/forms")


@router.get("/admin/forms", response_class=HTMLResponse, tags=["admin"])
async def list_forms(request: Request, _: Any = Depends(admin_guard)) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    forms = storage.forms.list_forms()

    sort = request.query_params.get("sort", "updated_at")
    order = request.query_params.get("order", "desc")
    if order not in ("asc", "desc"):
        order = "desc"
    reverse = order == "desc"

    if sort == "name":
        forms.sort(key=lambda f: (f.get("name") or "").lower(), reverse=reverse)
    elif sort == "status":
        forms.sort(key=lambda f: f.get("status") or "", reverse=reverse)
    else:
        sort = "updated_at"
        forms.sort(key=lambda f: str(f.get("updated_at") or ""), reverse=reverse)

    return templates.TemplateResponse(
        "admin_forms.html",
        {"request": request, "forms": forms, "sort": sort, "order": order},
    )


@router.get("/admin/forms/new", response_class=HTMLResponse, tags=["admin"])
async def new_form(request: Request, _: Any = Depends(admin_guard)) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    master_forms, master_field_catalog = build_master_field_catalog(storage)
    return templates.TemplateResponse(
        "admin_form_builder.html",
        {
            "request": request,
            "form": None,
            "fields": [],
            "fields_json": dumps_json([]),
            "master_forms_json": dumps_json(master_forms),
            "master_field_catalog_json": dumps_json(master_field_catalog),
            "errors": [],
        },
    )


@router.post("/admin/forms", response_class=HTMLResponse, tags=["admin"])
async def create_form(request: Request, _: Any = Depends(admin_guard)) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form_data = await request.form()
    name = str(form_data.get("name", "")).strip()
    description = str(form_data.get("description", "")).strip()
    fields_json = str(form_data.get("fields_json", ""))

    fields, errors = parse_fields_json(fields_json)
    master_forms, master_field_catalog = build_master_field_catalog(storage)
    if not name:
        errors.append("フォーム名は必須です")

    if errors:
        return templates.TemplateResponse(
            "admin_form_builder.html",
            {
                "request": request,
                "form": {"name": name, "description": description},
                "fields": fields,
                "fields_json": dumps_json(fields),
                "master_forms_json": dumps_json(master_forms),
                "master_field_catalog_json": dumps_json(master_field_catalog),
                "errors": errors,
            },
        )

    schema, field_order = schema_from_fields(fields)
    form_id = new_ulid()
    public_id = new_short_id()
    now = now_utc()
    webhook_url = str(form_data.get("webhook_url", "")).strip()
    if webhook_url and not is_valid_webhook_url(webhook_url):
        errors.append("Webhook URLはhttp://またはhttps://で始まる有効なURLを指定してください")
        return templates.TemplateResponse(
            "admin_form_builder.html",
            {
                "request": request,
                "form": {"name": name, "description": description, "webhook_url": webhook_url},
                "fields": fields,
                "fields_json": dumps_json(fields),
                "master_forms_json": dumps_json(master_forms),
                "master_field_catalog_json": dumps_json(master_field_catalog),
                "errors": errors,
            },
        )
    webhook_on_submit = bool(form_data.get("webhook_on_submit"))
    webhook_on_delete = bool(form_data.get("webhook_on_delete"))
    webhook_on_edit = bool(form_data.get("webhook_on_edit"))
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
            "created_at": now,
            "updated_at": now,
        }
    )
    return RedirectResponse(f"/admin/forms/{form_id}", status_code=303)


@router.get("/admin/forms/{form_id}", response_class=HTMLResponse, tags=["admin"])
async def edit_form(
    request: Request, form_id: str, _: Any = Depends(admin_guard)
) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    master_forms, master_field_catalog = build_master_field_catalog(
        storage, current_form_id=form_id
    )
    return templates.TemplateResponse(
        "admin_form_builder.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "fields_json": dumps_json(fields),
            "master_forms_json": dumps_json(master_forms),
            "master_field_catalog_json": dumps_json(master_field_catalog),
            "errors": [],
        },
    )


@router.post("/admin/forms/{form_id}", response_class=HTMLResponse, tags=["admin"])
async def update_form(
    request: Request, form_id: str, _: Any = Depends(admin_guard)
) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")

    form_data = await request.form()
    name = str(form_data.get("name", "")).strip()
    description = str(form_data.get("description", "")).strip()
    fields_json = str(form_data.get("fields_json", ""))

    fields, errors = parse_fields_json(fields_json)
    master_forms, master_field_catalog = build_master_field_catalog(
        storage, current_form_id=form_id
    )
    if not name:
        errors.append("フォーム名は必須です")

    if errors:
        return templates.TemplateResponse(
            "admin_form_builder.html",
            {
                "request": request,
                "form": {**form, "name": name, "description": description},
                "fields": fields,
                "fields_json": dumps_json(fields),
                "master_forms_json": dumps_json(master_forms),
                "master_field_catalog_json": dumps_json(master_field_catalog),
                "errors": errors,
            },
        )

    schema, field_order = schema_from_fields(fields)
    webhook_url = str(form_data.get("webhook_url", "")).strip()
    if webhook_url and not is_valid_webhook_url(webhook_url):
        errors.append("Webhook URLはhttp://またはhttps://で始まる有効なURLを指定してください")
        return templates.TemplateResponse(
            "admin_form_builder.html",
            {
                "request": request,
                "form": {**form, "name": name, "description": description, "webhook_url": webhook_url},
                "fields": fields,
                "fields_json": dumps_json(fields),
                "master_forms_json": dumps_json(master_forms),
                "master_field_catalog_json": dumps_json(master_field_catalog),
                "errors": errors,
            },
        )
    webhook_on_submit = bool(form_data.get("webhook_on_submit"))
    webhook_on_delete = bool(form_data.get("webhook_on_delete"))
    webhook_on_edit = bool(form_data.get("webhook_on_edit"))
    updated = storage.forms.update_form(
        form_id,
        {
            "name": name,
            "description": description,
            "schema_json": schema,
            "field_order": field_order,
            "webhook_url": webhook_url,
            "webhook_on_submit": webhook_on_submit,
            "webhook_on_delete": webhook_on_delete,
            "webhook_on_edit": webhook_on_edit,
            "updated_at": now_utc(),
        },
    )
    return RedirectResponse(f"/admin/forms/{updated['id']}", status_code=303)


@router.post("/admin/forms/{form_id}/publish", tags=["admin"])
async def publish_form(
    request: Request, form_id: str, _: Any = Depends(admin_guard)
) -> RedirectResponse:
    storage = request.app.state.storage
    storage.forms.set_status(form_id, "active")
    target = resolve_redirect_target(request.query_params.get("next"))
    return RedirectResponse(target, status_code=303)


@router.post("/admin/forms/{form_id}/stop", tags=["admin"])
async def stop_form(
    request: Request, form_id: str, _: Any = Depends(admin_guard)
) -> RedirectResponse:
    storage = request.app.state.storage
    storage.forms.set_status(form_id, "inactive")
    target = resolve_redirect_target(request.query_params.get("next"))
    return RedirectResponse(target, status_code=303)


@router.post("/admin/forms/{form_id}/delete", tags=["admin"])
async def delete_form(
    request: Request, form_id: str, _: Any = Depends(admin_guard)
) -> RedirectResponse:
    storage = request.app.state.storage
    storage.forms.delete_form(form_id)
    return RedirectResponse("/admin/forms", status_code=303)
