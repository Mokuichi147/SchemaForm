from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from jsonschema import Draft7Validator

from schemaform.file_formats import upload_matches_file_constraints
from schemaform.fields import clean_empty_recursive
from schemaform.filters import normalize_number, parse_bool
from schemaform.master import enrich_master_options, validate_master_references
from schemaform.schema import fields_from_schema
from schemaform.utils import new_ulid, now_utc

router = APIRouter()


async def save_upload(
    file_obj: Any,
    form_id: str,
    request: Request,
    file_format: str = "",
    allowed_extensions: list[str] | None = None,
) -> str:
    storage = request.app.state.storage
    settings = request.app.state.settings
    if not upload_matches_file_constraints(
        content_type=file_obj.content_type,
        filename=file_obj.filename,
        file_format=file_format,
        allowed_extensions=allowed_extensions or [],
    ):
        raise HTTPException(status_code=400, detail="ファイル種別が許可されていません")
    file_id = new_ulid()
    destination = settings.upload_dir / file_id
    content = await file_obj.read()
    if (
        settings.upload_max_bytes is not None
        and len(content) > settings.upload_max_bytes
    ):
        raise HTTPException(
            status_code=400, detail="ファイルサイズが上限を超えています"
        )
    destination.write_bytes(content)
    storage.files.create_file(
        {
            "id": file_id,
            "form_id": form_id,
            "original_name": file_obj.filename or "",
            "stored_path": str(destination),
            "content_type": file_obj.content_type or "",
            "size": len(content),
            "created_at": now_utc(),
        }
    )
    return file_id


@router.get("/f/{public_id}", response_class=HTMLResponse, tags=["public"])
async def public_form(request: Request, public_id: str) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form_by_public_id(public_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    enrich_master_options(storage, fields)
    inactive = form.get("status") != "active"
    errors = ["このフォームは停止中です"] if inactive else []
    return templates.TemplateResponse(
        "form_public.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "errors": errors,
            "inactive": inactive,
        },
    )


@router.post("/f/{public_id}", response_class=HTMLResponse, tags=["public"])
async def submit_form(request: Request, public_id: str) -> HTMLResponse:
    storage = request.app.state.storage
    templates = request.app.state.templates
    form = storage.forms.get_form_by_public_id(public_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    if form.get("status") != "active":
        fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
        enrich_master_options(storage, fields)
        return templates.TemplateResponse(
            "form_public.html",
            {
                "request": request,
                "form": form,
                "fields": fields,
                "errors": ["このフォームは停止中です"],
                "inactive": True,
            },
        )

    form_data = await request.form()
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    enrich_master_options(storage, fields)
    submission: dict[str, Any] = {}

    async def collect_fields(
        field_list: list[dict[str, Any]], target: dict[str, Any], prefix: str
    ) -> None:
        for field in field_list:
            key = field["key"]
            form_key = f"{prefix}{key}" if prefix else key
            field_type = field["type"]
            is_array = field.get("is_array", False)

            if field_type == "group":
                children = field.get("children") or []
                if is_array:
                    indices: set[int] = set()
                    form_prefix = f"{form_key}."
                    for k in form_data:
                        if k.startswith(form_prefix):
                            rest = k[len(form_prefix) :]
                            parts = rest.split(".", 1)
                            if parts[0].isdigit():
                                indices.add(int(parts[0]))
                    items: list[dict[str, Any]] = []
                    for idx in sorted(indices):
                        item: dict[str, Any] = {}
                        await collect_fields(children, item, f"{form_key}.{idx}.")
                        if item:
                            items.append(item)
                    target[key] = items
                else:
                    group_data: dict[str, Any] = {}
                    await collect_fields(children, group_data, f"{form_key}.")
                    target[key] = group_data
                continue

            if is_array:
                if field_type == "file":
                    uploads = form_data.getlist(form_key)
                    file_ids: list[str] = []
                    for upload in uploads:
                        if upload and getattr(upload, "filename", ""):
                            file_ids.append(
                                await save_upload(
                                    upload,
                                    form["id"],
                                    request,
                                    str(field.get("format", "")),
                                    field.get("allowed_extensions") or [],
                                )
                            )
                    target[key] = file_ids
                    continue

                values = [v for v in form_data.getlist(form_key) if v not in (None, "")]
                if field_type in {"number", "integer"}:
                    parsed = [
                        normalize_number(v, field_type == "integer")
                        for v in values
                        if normalize_number(v, field_type == "integer") is not None
                    ]
                    target[key] = parsed
                elif field_type == "boolean":
                    target[key] = [parse_bool(v) for v in values]
                else:
                    target[key] = values
            else:
                if field_type == "file":
                    upload = form_data.get(form_key)
                    if upload and getattr(upload, "filename", ""):
                        target[key] = await save_upload(
                            upload,
                            form["id"],
                            request,
                            str(field.get("format", "")),
                            field.get("allowed_extensions") or [],
                        )
                    else:
                        target[key] = None
                    continue

                raw_value = form_data.get(form_key)
                if field_type in {"number", "integer"}:
                    target[key] = normalize_number(raw_value, field_type == "integer")
                elif field_type == "boolean":
                    target[key] = parse_bool(raw_value)
                else:
                    target[key] = str(raw_value) if raw_value is not None else None

    await collect_fields(fields, submission, "")
    submission = clean_empty_recursive(submission) or {}

    validator = Draft7Validator(form["schema_json"])
    errors = sorted(validator.iter_errors(submission), key=lambda err: list(err.path))
    master_errors = validate_master_references(storage, fields, submission)
    if errors or master_errors:
        messages = [f"{error.message}" for error in errors] + master_errors
        return templates.TemplateResponse(
            "form_public.html",
            {
                "request": request,
                "form": form,
                "fields": fields,
                "errors": messages,
                "inactive": False,
            },
        )

    submission_id = new_ulid()
    created_at = now_utc()
    submission_record = {
        "id": submission_id,
        "form_id": form["id"],
        "data_json": submission,
        "created_at": created_at,
    }
    storage.submissions.create_submission(submission_record)

    if form.get("webhook_url") and form.get("webhook_on_submit"):
        from schemaform.webhook import send_webhook

        send_webhook(form["webhook_url"], "submit", form, submission_record)

    return templates.TemplateResponse(
        "submission_done.html",
        {"request": request, "form": form},
    )


@router.get("/files/{file_id}", tags=["public"])
async def download_file(request: Request, file_id: str) -> FileResponse:
    storage = request.app.state.storage
    settings = request.app.state.settings
    file_meta = storage.files.get_file(file_id)
    if not file_meta:
        raise HTTPException(status_code=404, detail="ファイルが見つかりません")
    path = Path(file_meta["stored_path"]).resolve()
    if settings.upload_dir.resolve() not in path.parents:
        raise HTTPException(status_code=400, detail="不正なファイルパスです")
    return FileResponse(path, filename=file_meta.get("original_name") or file_id)
