from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from schemaform.filters import (
    apply_filters,
    collect_file_ids,
    decode_cursor,
    encode_cursor,
    ensure_aware,
    resolve_file_names,
)
from schemaform.master import validate_master_references
from schemaform.schema import (
    fields_from_schema,
    normalize_field_order,
    sanitize_form_output,
)
from schemaform.utils import new_short_id, new_ulid, now_utc, to_iso
from schemaform.webhook import is_valid_webhook_url

router = APIRouter()


@router.get("/api/forms", tags=["api/forms"])
async def api_list_forms(request: Request) -> JSONResponse:
    storage = request.app.state.storage
    forms = storage.forms.list_forms()
    return JSONResponse([sanitize_form_output(form) for form in forms])


@router.post("/api/forms", tags=["api/forms"])
async def api_create_form(request: Request) -> JSONResponse:
    storage = request.app.state.storage
    payload = await request.json()
    name = str(payload.get("name", "")).strip()
    description = str(payload.get("description", "")).strip()
    schema = payload.get("schema_json") or {}
    if not isinstance(schema, dict):
        raise HTTPException(status_code=400, detail="schema_jsonが不正です")
    if not name:
        raise HTTPException(status_code=400, detail="nameは必須です")
    field_order = normalize_field_order(schema, payload.get("field_order"))

    webhook_url = str(payload.get("webhook_url", "")).strip()
    if webhook_url and not is_valid_webhook_url(webhook_url):
        raise HTTPException(status_code=400, detail="webhook_urlが不正です")
    webhook_on_submit = bool(payload.get("webhook_on_submit"))
    webhook_on_delete = bool(payload.get("webhook_on_delete"))
    webhook_on_edit = bool(payload.get("webhook_on_edit"))

    form_id = new_ulid()
    public_id = new_short_id()
    now = now_utc()
    storage.forms.create_form(
        {
            "id": form_id,
            "public_id": public_id,
            "name": name,
            "description": description,
            "status": payload.get("status", "inactive"),
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
    form = storage.forms.get_form(form_id)
    return JSONResponse(sanitize_form_output(form or {}))


@router.put("/api/forms/{form_id}", tags=["api/forms"])
async def api_update_form(form_id: str, request: Request) -> JSONResponse:
    storage = request.app.state.storage
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    payload = await request.json()
    updates: dict[str, Any] = {}
    if "name" in payload:
        name = str(payload.get("name", "")).strip()
        if not name:
            raise HTTPException(status_code=400, detail="nameは必須です")
        updates["name"] = name
    if "description" in payload:
        updates["description"] = str(payload.get("description", "")).strip()
    if "schema_json" in payload:
        schema = payload.get("schema_json")
        if not isinstance(schema, dict):
            raise HTTPException(status_code=400, detail="schema_jsonが不正です")
        updates["schema_json"] = schema
        updates["field_order"] = normalize_field_order(
            schema, payload.get("field_order")
        )
    if "status" in payload:
        updates["status"] = str(payload.get("status") or "inactive")
    if "webhook_url" in payload:
        webhook_url = str(payload.get("webhook_url", "")).strip()
        if webhook_url and not is_valid_webhook_url(webhook_url):
            raise HTTPException(status_code=400, detail="webhook_urlが不正です")
        updates["webhook_url"] = webhook_url
    if "webhook_on_submit" in payload:
        updates["webhook_on_submit"] = bool(payload.get("webhook_on_submit"))
    if "webhook_on_delete" in payload:
        updates["webhook_on_delete"] = bool(payload.get("webhook_on_delete"))
    if "webhook_on_edit" in payload:
        updates["webhook_on_edit"] = bool(payload.get("webhook_on_edit"))
    updates["updated_at"] = now_utc()
    updated = storage.forms.update_form(form_id, updates)
    return JSONResponse(sanitize_form_output(updated))


@router.post("/api/public/forms/{public_id}/submissions", tags=["api/submissions"])
async def api_submit_form(public_id: str, request: Request) -> JSONResponse:
    storage = request.app.state.storage
    from jsonschema import Draft7Validator

    form = storage.forms.get_form_by_public_id(public_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    if form.get("status") != "active":
        raise HTTPException(status_code=400, detail="このフォームは停止中です")
    payload = await request.json()
    data = payload.get("data_json", payload)
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="data_jsonが不正です")

    validator = Draft7Validator(form["schema_json"])
    errors = sorted(validator.iter_errors(data), key=lambda err: list(err.path))
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    master_errors = validate_master_references(storage, fields, data)
    if errors or master_errors:
        raise HTTPException(status_code=400, detail="バリデーションに失敗しました")

    submission_id = new_ulid()
    created_at = now_utc()
    submission = {
        "id": submission_id,
        "form_id": form["id"],
        "data_json": data,
        "created_at": created_at,
    }
    storage.submissions.create_submission(submission)

    if form.get("webhook_url") and form.get("webhook_on_submit"):
        from schemaform.webhook import send_webhook

        await send_webhook(form["webhook_url"], "submit", form, submission)

    return JSONResponse(
        {"submission_id": submission_id, "created_at": to_iso(created_at)}
    )


@router.get("/api/forms/{form_id}/submissions", tags=["api/submissions"])
async def api_list_submissions(request: Request, form_id: str) -> JSONResponse:
    storage = request.app.state.storage
    form = storage.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    submissions = storage.submissions.list_submissions(form_id)
    file_ids = collect_file_ids(submissions, fields)
    file_names = resolve_file_names(storage.files, file_ids)

    filtered = apply_filters(
        submissions, fields, dict(request.query_params), file_names=file_names
    )
    filtered.sort(key=lambda item: (item["created_at"], item["id"]), reverse=True)

    cursor_raw = request.query_params.get("cursor")
    limit = int(request.query_params.get("limit", 50))
    if cursor_raw:
        cursor = decode_cursor(cursor_raw)
        if cursor:
            cursor_dt, cursor_id = cursor
            filtered = [
                item
                for item in filtered
                if (ensure_aware(item["created_at"]) < cursor_dt)
                or (
                    ensure_aware(item["created_at"]) == cursor_dt
                    and item["id"] < cursor_id
                )
            ]
        else:
            raise HTTPException(status_code=400, detail="cursorが不正です")

    page_items = filtered[:limit]
    response_items = [
        {
            "id": item["id"],
            "form_id": item["form_id"],
            "data_json": item.get("data_json", {}),
            "created_at": to_iso(item["created_at"]),
            "updated_at": to_iso(item["updated_at"]) if item.get("updated_at") else None,
        }
        for item in page_items
    ]
    headers: dict[str, str] = {}
    if len(page_items) == limit:
        last = page_items[-1]
        headers["X-Next-Cursor"] = encode_cursor(last["created_at"], last["id"])
    return JSONResponse(response_items, headers=headers)
