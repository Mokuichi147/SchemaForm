from __future__ import annotations

import csv
import io
import os
import re
import secrets
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol
from urllib.parse import urlencode

import orjson
import typer
import ulid
import base64
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from filelock import FileLock
from jsonschema import Draft7Validator
from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from tinydb import Query, TinyDB

ALLOWED_TYPES = {"string", "number", "integer", "boolean", "enum", "file", "datetime", "date", "time", "group"}
KEY_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def flatten_fields(
    fields: list[dict[str, Any]],
    prefix: str = "",
    label_prefix: str = "",
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for field in fields:
        key = f"{prefix}{field['key']}" if prefix else field["key"]
        label = f"{label_prefix}{field.get('label') or field['key']}" if label_prefix else (field.get("label") or field["key"])
        if field.get("type") == "group":
            if field.get("is_array"):
                result.append({**field, "flat_key": key, "flat_label": label})
            else:
                children = field.get("children") or []
                result.extend(flatten_fields(children, prefix=key + ".", label_prefix=label + "."))
        else:
            result.append({**field, "flat_key": key, "flat_label": label})
    return result


def _build_key_label_map(children: list[dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for child in children:
        mapping[child["key"]] = child.get("label") or child["key"]
    return mapping


def format_array_group_value(value: Any, children: list[dict[str, Any]]) -> str:
    if not value or not isinstance(value, list):
        return ""
    key_map = _build_key_label_map(children)
    result: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            result.append({key_map.get(k, k): v for k, v in item.items()})
        else:
            result.append(item)
    return dumps_json(result)


def get_nested_value(data: dict[str, Any], dotted_key: str) -> Any:
    parts = dotted_key.split(".")
    current: Any = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def set_nested_value(data: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    current = data
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def clean_empty_recursive(data: Any) -> Any:
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            result = clean_empty_recursive(v)
            if result is not None and result != "":
                cleaned[k] = result
        return cleaned if cleaned else None
    if isinstance(data, list):
        cleaned_list = []
        for item in data:
            result = clean_empty_recursive(item)
            if result is not None and result != "":
                cleaned_list.append(result)
        return cleaned_list if cleaned_list else None
    return data


class Settings:
    def __init__(self) -> None:
        self.storage_backend = os.getenv("STORAGE_BACKEND", "sqlite").lower()
        self.sqlite_path = Path(os.getenv("SQLITE_PATH", "./data/app.db"))
        self.json_path = Path(os.getenv("JSON_PATH", "./data/jsonstore.json"))
        self.upload_dir = Path(os.getenv("UPLOAD_DIR", "./data/uploads"))
        max_bytes = os.getenv("UPLOAD_MAX_BYTES")
        self.upload_max_bytes = int(max_bytes) if max_bytes else None
        self.auth_mode = os.getenv("AUTH_MODE", "none").lower()
        self.host = os.getenv("HOST", "0.0.0.0")
        port_value = os.getenv("PORT", "8000")
        try:
            self.port = int(port_value)
        except ValueError:
            self.port = 8000


SETTINGS = Settings()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return now_utc()
    return now_utc()


def dumps_json(value: Any) -> str:
    return orjson.dumps(value).decode("utf-8")


def loads_json(value: str | None) -> Any:
    if not value:
        return None
    return orjson.loads(value)


def new_ulid() -> str:
    value = ulid.new()
    return getattr(value, "str", str(value))


def new_short_id() -> str:
    return secrets.token_urlsafe(8)


def generate_field_key(existing: set[str]) -> str:
    while True:
        candidate = f"f_{secrets.token_hex(6)}"
        if candidate not in existing and KEY_PATTERN.match(candidate):
            return candidate


def ensure_dirs() -> None:
    SETTINGS.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS.json_path.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS.upload_dir.mkdir(parents=True, exist_ok=True)


class FormRepository(Protocol):
    def list_forms(self) -> list[dict[str, Any]]: ...

    def get_form(self, form_id: str) -> dict[str, Any] | None: ...

    def get_form_by_public_id(self, public_id: str) -> dict[str, Any] | None: ...

    def create_form(self, form: dict[str, Any]) -> None: ...

    def update_form(self, form_id: str, updates: dict[str, Any]) -> dict[str, Any]: ...

    def set_status(self, form_id: str, status: str) -> None: ...


class SubmissionRepository(Protocol):
    def list_submissions(self, form_id: str) -> list[dict[str, Any]]: ...

    def create_submission(self, submission: dict[str, Any]) -> None: ...

    def delete_submission(self, submission_id: str) -> None: ...


class FileRepository(Protocol):
    def create_file(self, file_meta: dict[str, Any]) -> None: ...

    def get_file(self, file_id: str) -> dict[str, Any] | None: ...


class Storage(Protocol):
    forms: FormRepository
    submissions: SubmissionRepository
    files: FileRepository


class Base(DeclarativeBase):
    pass


class FormModel(Base):
    __tablename__ = "forms"

    id = Column(String, primary_key=True)
    public_id = Column(String, unique=True, index=True)
    name = Column(String)
    description = Column(Text)
    status = Column(String)
    schema_json = Column(Text)
    field_order = Column(Text)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class SubmissionModel(Base):
    __tablename__ = "submissions"

    id = Column(String, primary_key=True)
    form_id = Column(String, index=True)
    data_json = Column(Text)
    created_at = Column(DateTime)


class FileModel(Base):
    __tablename__ = "files"

    id = Column(String, primary_key=True)
    form_id = Column(String, index=True)
    original_name = Column(String)
    stored_path = Column(Text)
    content_type = Column(String)
    size = Column(Integer)
    created_at = Column(DateTime)


class SQLiteFormRepo:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._Session = session_factory

    def list_forms(self) -> list[dict[str, Any]]:
        with self._Session() as session:
            rows = session.query(FormModel).order_by(FormModel.updated_at.desc()).all()
            return [self._to_dict(row) for row in rows]

    def get_form(self, form_id: str) -> dict[str, Any] | None:
        with self._Session() as session:
            row = session.get(FormModel, form_id)
            return self._to_dict(row) if row else None

    def get_form_by_public_id(self, public_id: str) -> dict[str, Any] | None:
        with self._Session() as session:
            row = session.query(FormModel).filter(FormModel.public_id == public_id).first()
            return self._to_dict(row) if row else None

    def create_form(self, form: dict[str, Any]) -> None:
        with self._Session() as session:
            row = FormModel(
                id=form["id"],
                public_id=form["public_id"],
                name=form["name"],
                description=form["description"],
                status=form["status"],
                schema_json=dumps_json(form["schema_json"]),
                field_order=dumps_json(form["field_order"]),
                created_at=form["created_at"],
                updated_at=form["updated_at"],
            )
            session.add(row)
            session.commit()

    def update_form(self, form_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        with self._Session() as session:
            row = session.get(FormModel, form_id)
            if not row:
                raise KeyError(form_id)
            for key, value in updates.items():
                if key in {"schema_json", "field_order"}:
                    setattr(row, key, dumps_json(value))
                else:
                    setattr(row, key, value)
            session.commit()
            session.refresh(row)
            return self._to_dict(row)

    def set_status(self, form_id: str, status: str) -> None:
        with self._Session() as session:
            row = session.get(FormModel, form_id)
            if not row:
                raise KeyError(form_id)
            row.status = status
            row.updated_at = now_utc()
            session.commit()

    @staticmethod
    def _to_dict(row: FormModel) -> dict[str, Any]:
        return {
            "id": row.id,
            "public_id": row.public_id,
            "name": row.name,
            "description": row.description or "",
            "status": row.status,
            "schema_json": loads_json(row.schema_json) or {},
            "field_order": loads_json(row.field_order) or [],
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }


class SQLiteSubmissionRepo:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._Session = session_factory

    def list_submissions(self, form_id: str) -> list[dict[str, Any]]:
        with self._Session() as session:
            rows = (
                session.query(SubmissionModel)
                .filter(SubmissionModel.form_id == form_id)
                .order_by(SubmissionModel.created_at.desc())
                .all()
            )
            return [self._to_dict(row) for row in rows]

    def create_submission(self, submission: dict[str, Any]) -> None:
        with self._Session() as session:
            row = SubmissionModel(
                id=submission["id"],
                form_id=submission["form_id"],
                data_json=dumps_json(submission["data_json"]),
                created_at=submission["created_at"],
            )
            session.add(row)
            session.commit()

    def delete_submission(self, submission_id: str) -> None:
        with self._Session() as session:
            row = session.get(SubmissionModel, submission_id)
            if row:
                session.delete(row)
                session.commit()

    @staticmethod
    def _to_dict(row: SubmissionModel) -> dict[str, Any]:
        return {
            "id": row.id,
            "form_id": row.form_id,
            "data_json": loads_json(row.data_json) or {},
            "created_at": row.created_at,
        }


class SQLiteFileRepo:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._Session = session_factory

    def create_file(self, file_meta: dict[str, Any]) -> None:
        with self._Session() as session:
            row = FileModel(
                id=file_meta["id"],
                form_id=file_meta["form_id"],
                original_name=file_meta["original_name"],
                stored_path=file_meta["stored_path"],
                content_type=file_meta["content_type"],
                size=file_meta["size"],
                created_at=file_meta["created_at"],
            )
            session.add(row)
            session.commit()

    def get_file(self, file_id: str) -> dict[str, Any] | None:
        with self._Session() as session:
            row = session.get(FileModel, file_id)
            if not row:
                return None
            return {
                "id": row.id,
                "form_id": row.form_id,
                "original_name": row.original_name,
                "stored_path": row.stored_path,
                "content_type": row.content_type,
                "size": row.size,
                "created_at": row.created_at,
            }


class SQLiteStorage:
    def __init__(self, db_path: Path) -> None:
        self._engine = create_engine(f"sqlite:///{db_path}", future=True)
        self._Session = sessionmaker(self._engine, expire_on_commit=False)
        Base.metadata.create_all(self._engine)
        self.forms = SQLiteFormRepo(self._Session)
        self.submissions = SQLiteSubmissionRepo(self._Session)
        self.files = SQLiteFileRepo(self._Session)


class JSONRepoBase:
    def __init__(self, path: Path, lock: FileLock) -> None:
        self._path = path
        self._lock = lock

    @contextmanager
    def _db(self) -> Iterable[TinyDB]:
        with self._lock:
            db = TinyDB(self._path)
            try:
                yield db
            finally:
                db.close()


class JSONFormRepo(JSONRepoBase):
    def list_forms(self) -> list[dict[str, Any]]:
        with self._db() as db:
            items = db.table("forms").all()
        forms = [self._from_record(item) for item in items]
        return sorted(forms, key=lambda x: x["updated_at"], reverse=True)

    def get_form(self, form_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            table = db.table("forms")
            item = table.get(Query().id == form_id)
        return self._from_record(item) if item else None

    def get_form_by_public_id(self, public_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            table = db.table("forms")
            item = table.get(Query().public_id == public_id)
        return self._from_record(item) if item else None

    def create_form(self, form: dict[str, Any]) -> None:
        record = self._to_record(form)
        with self._db() as db:
            db.table("forms").insert(record)

    def update_form(self, form_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        with self._db() as db:
            table = db.table("forms")
            item = table.get(Query().id == form_id)
            if not item:
                raise KeyError(form_id)
            item.update(self._to_record(updates, partial=True))
            table.update(item, Query().id == form_id)
        return self._from_record(item)

    def set_status(self, form_id: str, status: str) -> None:
        with self._db() as db:
            table = db.table("forms")
            item = table.get(Query().id == form_id)
            if not item:
                raise KeyError(form_id)
            item["status"] = status
            item["updated_at"] = to_iso(now_utc())
            table.update(item, Query().id == form_id)

    @staticmethod
    def _to_record(form: dict[str, Any], partial: bool = False) -> dict[str, Any]:
        record: dict[str, Any] = {}
        for key, value in form.items():
            if key in {"created_at", "updated_at"}:
                record[key] = to_iso(value) if isinstance(value, datetime) else value
            else:
                record[key] = value
        if not partial:
            record.setdefault("created_at", to_iso(now_utc()))
            record.setdefault("updated_at", to_iso(now_utc()))
        return record

    @staticmethod
    def _from_record(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": record["id"],
            "public_id": record["public_id"],
            "name": record["name"],
            "description": record.get("description", ""),
            "status": record.get("status", "inactive"),
            "schema_json": record.get("schema_json", {}),
            "field_order": record.get("field_order", []),
            "created_at": parse_dt(record.get("created_at")),
            "updated_at": parse_dt(record.get("updated_at")),
        }


class JSONSubmissionRepo(JSONRepoBase):
    def list_submissions(self, form_id: str) -> list[dict[str, Any]]:
        with self._db() as db:
            items = db.table("submissions").search(Query().form_id == form_id)
        submissions = [self._from_record(item) for item in items]
        return sorted(submissions, key=lambda x: x["created_at"], reverse=True)

    def create_submission(self, submission: dict[str, Any]) -> None:
        record = self._to_record(submission)
        with self._db() as db:
            db.table("submissions").insert(record)

    def delete_submission(self, submission_id: str) -> None:
        with self._db() as db:
            db.table("submissions").remove(Query().id == submission_id)

    @staticmethod
    def _to_record(submission: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": submission["id"],
            "form_id": submission["form_id"],
            "data_json": submission["data_json"],
            "created_at": to_iso(submission["created_at"]),
        }

    @staticmethod
    def _from_record(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": record["id"],
            "form_id": record["form_id"],
            "data_json": record.get("data_json", {}),
            "created_at": parse_dt(record.get("created_at")),
        }


class JSONFileRepo(JSONRepoBase):
    def create_file(self, file_meta: dict[str, Any]) -> None:
        record = self._to_record(file_meta)
        with self._db() as db:
            db.table("files").insert(record)

    def get_file(self, file_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            item = db.table("files").get(Query().id == file_id)
        return self._from_record(item) if item else None

    @staticmethod
    def _to_record(file_meta: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": file_meta["id"],
            "form_id": file_meta["form_id"],
            "original_name": file_meta["original_name"],
            "stored_path": file_meta["stored_path"],
            "content_type": file_meta["content_type"],
            "size": file_meta["size"],
            "created_at": to_iso(file_meta["created_at"]),
        }

    @staticmethod
    def _from_record(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": record["id"],
            "form_id": record["form_id"],
            "original_name": record.get("original_name", ""),
            "stored_path": record.get("stored_path", ""),
            "content_type": record.get("content_type", ""),
            "size": record.get("size", 0),
            "created_at": parse_dt(record.get("created_at")),
        }


class JSONStorage:
    def __init__(self, path: Path) -> None:
        self._lock = FileLock(f"{path}.lock")
        self.forms = JSONFormRepo(path, self._lock)
        self.submissions = JSONSubmissionRepo(path, self._lock)
        self.files = JSONFileRepo(path, self._lock)


def init_storage() -> Storage:
    ensure_dirs()
    if SETTINGS.storage_backend == "json":
        return JSONStorage(SETTINGS.json_path)
    return SQLiteStorage(SETTINGS.sqlite_path)


STORAGE = init_storage()


class AuthProvider(Protocol):
    def require_admin(self, request: Request) -> None: ...


class NoAuthProvider:
    def require_admin(self, request: Request) -> None:
        return None


class LDAPAuthProvider:
    def require_admin(self, request: Request) -> None:
        raise HTTPException(status_code=501, detail="LDAP認証は未実装です")


def get_auth_provider() -> AuthProvider:
    if SETTINGS.auth_mode == "ldap":
        return LDAPAuthProvider()
    return NoAuthProvider()


AUTH_PROVIDER = get_auth_provider()


def admin_guard(request: Request) -> None:
    AUTH_PROVIDER.require_admin(request)


app = FastAPI(
    openapi_tags=[
        {"name": "admin", "description": "管理画面（HTML）"},
        {"name": "public", "description": "公開フォーム（HTML）"},
        {"name": "api/forms", "description": "REST API: フォーム"},
        {"name": "api/submissions", "description": "REST API: 送信"},
        {"name": "system", "description": "システム"},
    ]
)
cli = typer.Typer(add_completion=False)

templates = Jinja2Templates(directory="templates")


def field_input_type(field: dict[str, Any]) -> str:
    field_type = field["type"]
    if field_type == "datetime":
        return "text"
    if field_type == "date":
        return "text"
    if field_type == "time":
        return "text"
    if field_type == "string":
        fmt = field.get("format")
        if fmt in {"email", "url"}:
            return fmt
        if fmt in {"date", "datetime-local"}:
            return "text"
        return "text"
    if field_type in {"number", "integer"}:
        return "number"
    if field_type == "file":
        return "file"
    return "text"


templates.env.globals["field_input_type"] = field_input_type


def field_picker(field: dict[str, Any]) -> str:
    field_type = field.get("type")
    if field_type == "datetime":
        return "datetime-local"
    if field_type == "date":
        return "date"
    if field_type == "time":
        return "time"
    if field_type == "string" and field.get("format") in {"datetime-local"}:
        return field["format"]
    return ""


templates.env.globals["field_picker"] = field_picker


def format_dt(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone().strftime("%Y-%m-%d %H:%M")
    return str(value or "")


templates.env.globals["format_dt"] = format_dt


def iso_dt(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    return ""


templates.env.globals["iso_dt"] = iso_dt


def build_query(base: dict[str, Any], **overrides: Any) -> str:
    params = {k: v for k, v in base.items() if v not in (None, "")}
    for key, value in overrides.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = str(value)
    return urlencode(params, doseq=True)


templates.env.globals["build_query"] = build_query


@app.get("/", response_class=HTMLResponse, tags=["admin"])
async def home(request: Request) -> HTMLResponse:
    return RedirectResponse("/admin/forms")


@app.get("/admin/forms", response_class=HTMLResponse, tags=["admin"])
async def list_forms(request: Request, _: Any = Depends(admin_guard)) -> HTMLResponse:
    forms = STORAGE.forms.list_forms()
    return templates.TemplateResponse(
        "admin_forms.html",
        {"request": request, "forms": forms},
    )


@app.get("/admin/forms/new", response_class=HTMLResponse, tags=["admin"])
async def new_form(request: Request, _: Any = Depends(admin_guard)) -> HTMLResponse:
    return templates.TemplateResponse(
        "admin_form_builder.html",
        {
            "request": request,
            "form": None,
            "fields": [],
            "fields_json": dumps_json([]),
            "errors": [],
        },
    )


def parse_fields_json(fields_json: str) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    try:
        raw_fields = orjson.loads(fields_json) if fields_json else []
    except orjson.JSONDecodeError:
        return [], ["フィールド定義の解析に失敗しました"]

    seen_keys: set[str] = set()

    def _parse_recursive(raw_list: list, prefix: str = "") -> list[dict[str, Any]]:
        fields: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_list, start=1):
            loc = f"{prefix}{index}行目" if prefix else f"{index}行目"
            key = str(raw.get("key", "")).strip()
            label = str(raw.get("label", "")).strip()

            if not label:
                errors.append(f"{loc}: ラベルは必須です")

            if not key:
                key = generate_field_key(seen_keys)
            if not KEY_PATTERN.match(key):
                errors.append(f"{loc}: キーは英字で始まり英数字/アンダースコアのみです")
            if key in seen_keys:
                errors.append(f"{loc}: キーが重複しています ({key})")
            else:
                seen_keys.add(key)

            field_type = str(raw.get("type", "")).strip()
            is_array = bool(raw.get("is_array"))
            items_type = str(raw.get("items_type", "")).strip() if is_array else ""

            if field_type not in ALLOWED_TYPES:
                errors.append(f"{loc}: 種類が不正です ({field_type})")

            children: list[dict[str, Any]] = []
            if field_type == "group":
                raw_children = raw.get("children") or []
                children = _parse_recursive(raw_children, prefix=f"{loc}.")
                if not children and not errors:
                    errors.append(f"{loc}: グループには子フィールドが必要です")
            else:
                if is_array and items_type not in ALLOWED_TYPES:
                    errors.append(f"{loc}: 配列の要素型が不正です ({items_type})")

            enum_values = [
                value.strip()
                for value in (raw.get("enum") or [])
                if isinstance(value, str) and value.strip()
            ]
            if (field_type == "enum" or items_type == "enum") and not enum_values:
                errors.append(f"{loc}: enumは値を指定してください")

            min_raw = str(raw.get("min", "")).strip()
            max_raw = str(raw.get("max", "")).strip()
            min_value = float(min_raw) if min_raw else None
            max_value = float(max_raw) if max_raw else None

            fields.append(
                {
                    "key": key,
                    "label": label,
                    "type": field_type,
                    "required": bool(raw.get("required")),
                    "description": str(raw.get("description", "")).strip(),
                    "placeholder": str(raw.get("placeholder", "")).strip(),
                    "enum": enum_values,
                    "min": min_value,
                    "max": max_value,
                    "format": str(raw.get("format", "")).strip(),
                    "is_array": is_array,
                    "items_type": items_type,
                    "multiline": bool(raw.get("multiline")),
                    "children": children,
                }
            )
        return fields

    fields = _parse_recursive(raw_fields)

    if not fields:
        errors.append("最低1つのフィールドが必要です")

    return fields, errors


def build_property(field: dict[str, Any]) -> dict[str, Any]:
    def build_item(item_type: str) -> dict[str, Any]:
        if item_type == "file":
            return {"type": "string", "format": "binary"}
        if item_type == "datetime":
            return {"type": "string", "format": "datetime-local"}
        if item_type == "date":
            return {"type": "string", "format": "date"}
        if item_type == "time":
            return {"type": "string", "format": "time"}
        if item_type == "enum":
            return {"type": "string", "enum": field.get("enum", [])}
        payload: dict[str, Any] = {"type": item_type}
        if item_type in {"number", "integer"}:
            if field.get("min") is not None:
                payload["minimum"] = field["min"]
            if field.get("max") is not None:
                payload["maximum"] = field["max"]
        if item_type == "string" and field.get("format"):
            payload["format"] = field["format"]
        return payload

    if field["type"] == "group":
        children = field.get("children") or []
        child_schema, child_order = schema_from_fields(children)
        obj: dict[str, Any] = {
            "type": "object",
            "properties": child_schema.get("properties", {}),
            "x-field-type": "group",
            "x-field-order": child_order,
        }
        if child_schema.get("required"):
            obj["required"] = child_schema["required"]
        if field.get("is_array"):
            prop: dict[str, Any] = {"type": "array", "items": obj}
        else:
            prop = obj
    elif field["is_array"]:
        item_type = field.get("items_type") or "string"
        prop = {"type": "array", "items": build_item(item_type)}
    else:
        prop = build_item(field["type"])

    prop["title"] = field.get("label") or field["key"]
    if field.get("description"):
        prop["description"] = field["description"]
    if field.get("placeholder"):
        prop["x-placeholder"] = field["placeholder"]
    if field.get("multiline"):
        prop["x-multiline"] = True

    return prop


def normalize_field_order(schema: dict[str, Any], field_order: list[str] | None) -> list[str]:
    properties = list(schema.get("properties", {}).keys())
    if not field_order:
        return properties
    seen: set[str] = set()
    ordered: list[str] = []
    for key in field_order:
        if key in properties and key not in seen:
            ordered.append(key)
            seen.add(key)
    for key in properties:
        if key not in seen:
            ordered.append(key)
    return ordered


def sanitize_form_output(form: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": form["id"],
        "public_id": form["public_id"],
        "name": form.get("name", ""),
        "description": form.get("description", ""),
        "status": form.get("status", "inactive"),
        "schema_json": form.get("schema_json", {}),
        "field_order": form.get("field_order", []),
        "created_at": to_iso(form.get("created_at", now_utc())),
        "updated_at": to_iso(form.get("updated_at", now_utc())),
    }


def schema_from_fields(fields: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    properties: dict[str, Any] = {}
    required: list[str] = []
    field_order: list[str] = []
    for field in fields:
        key = field["key"]
        field_order.append(key)
        properties[key] = build_property(field)
        if field.get("required"):
            required.append(key)
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema, field_order


def fields_from_schema(schema: dict[str, Any], field_order: list[str]) -> list[dict[str, Any]]:
    properties = schema.get("properties", {})
    order = field_order or list(properties.keys())
    fields: list[dict[str, Any]] = []

    for key in order:
        prop = properties.get(key, {})
        is_array = prop.get("type") == "array"
        target = prop.get("items", {}) if is_array else prop

        is_group = (
            target.get("x-field-type") == "group"
            or (target.get("type") == "object" and "properties" in target)
        )

        if is_group:
            child_order = target.get("x-field-order", list(target.get("properties", {}).keys()))
            children = fields_from_schema(target, child_order)
            fields.append(
                {
                    "key": key,
                    "label": prop.get("title", ""),
                    "type": "group",
                    "required": key in schema.get("required", []),
                    "description": prop.get("description", ""),
                    "placeholder": "",
                    "enum": [],
                    "min": None,
                    "max": None,
                    "format": "",
                    "is_array": is_array,
                    "items_type": "",
                    "multiline": False,
                    "children": children,
                }
            )
            continue

        field_type = target.get("type", "string")
        if target.get("format") == "datetime-local":
            field_type = "datetime"
        if target.get("format") == "date":
            field_type = "date"
        if target.get("format") == "time":
            field_type = "time"
        if "enum" in target:
            field_type = "enum"
        if target.get("format") == "binary":
            field_type = "file"

        fields.append(
            {
                "key": key,
                "label": prop.get("title", ""),
                "type": field_type,
                "required": key in schema.get("required", []),
                "description": prop.get("description", ""),
                "placeholder": prop.get("x-placeholder", ""),
                "enum": target.get("enum", []),
                "min": target.get("minimum"),
                "max": target.get("maximum"),
                "format": target.get("format", "") if field_type == "string" else "",
                "is_array": is_array,
                "items_type": field_type if is_array else "",
                "multiline": prop.get("x-multiline", False),
                "children": [],
            }
        )
    return fields


def parse_bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "on", "yes"}


def normalize_number(value: Any, is_int: bool) -> Any:
    if value in (None, ""):
        return None
    try:
        return int(value) if is_int else float(value)
    except ValueError:
        return None


def ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_query_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def collect_file_ids(
    submissions: list[dict[str, Any]], fields: list[dict[str, Any]]
) -> set[str]:
    ids: set[str] = set()
    flat = flatten_fields(fields)
    file_keys = {f["flat_key"] for f in flat if f["type"] == "file"}
    for submission in submissions:
        data = submission.get("data_json", {})
        for key in file_keys:
            value = get_nested_value(data, key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        ids.add(item)
            elif isinstance(value, str):
                ids.add(value)
    return ids


def resolve_file_names(file_repo: FileRepository, file_ids: Iterable[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for file_id in file_ids:
        file_meta = file_repo.get_file(file_id)
        if file_meta:
            mapping[file_id] = file_meta.get("original_name", "")
    return mapping


def value_to_text(value: Any, file_names: dict[str, str], use_file_names: bool) -> str:
    if isinstance(value, list):
        return ", ".join(
            value_to_text(item, file_names, use_file_names) for item in value if item is not None
        )
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if use_file_names and isinstance(value, str) and value in file_names:
        return file_names[value]
    return str(value)


def apply_filters(
    submissions: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    query_params: dict[str, Any],
) -> list[dict[str, Any]]:
    q = str(query_params.get("q", "")).strip().lower()
    from_dt = parse_query_datetime(query_params.get("submitted_from"))
    to_dt = parse_query_datetime(query_params.get("submitted_to"))
    flat_fields = flatten_fields(fields)

    def matches_free_text(data: dict[str, Any]) -> bool:
        if not q:
            return True
        combined = " ".join(str(value) for value in data.values()).lower()
        return q in combined

    filtered: list[dict[str, Any]] = []
    for submission in submissions:
        created_at = submission.get("created_at")
        if created_at and (from_dt or to_dt):
            created_value = ensure_aware(created_at) if isinstance(created_at, datetime) else None
            if created_value is not None:
                if from_dt and created_value < ensure_aware(from_dt):
                    continue
                if to_dt and created_value > ensure_aware(to_dt):
                    continue
        data = submission.get("data_json", {})
        if not matches_free_text(data):
            continue
        ok = True
        for field in flat_fields:
            flat_key = field["flat_key"]
            param_key = f"f_{flat_key.replace('.', '__')}"
            value = get_nested_value(data, flat_key)
            field_type = field["type"]
            is_array = field.get("is_array", False)

            if field_type == "group":
                continue

            if is_array:
                filter_value = str(query_params.get(param_key, "")).strip()
                if not filter_value:
                    continue
                items = value or []
                if field_type == "enum":
                    if filter_value not in items:
                        ok = False
                        break
                else:
                    if not any(filter_value.lower() in str(item).lower() for item in items):
                        ok = False
                        break
                continue

            if field_type in {"string", "enum", "file", "datetime", "date", "time"}:
                filter_value = str(query_params.get(param_key, "")).strip()
                if not filter_value:
                    continue
                if field_type == "enum":
                    if str(value) != filter_value:
                        ok = False
                        break
                else:
                    if filter_value.lower() not in str(value or "").lower():
                        ok = False
                        break
            elif field_type in {"number", "integer"}:
                min_val = query_params.get(f"{param_key}_min")
                max_val = query_params.get(f"{param_key}_max")
                if min_val not in (None, "") or max_val not in (None, ""):
                    if value is None:
                        ok = False
                        break
                    if min_val not in (None, "") and value < float(min_val):
                        ok = False
                        break
                    if max_val not in (None, "") and value > float(max_val):
                        ok = False
                        break
            elif field_type == "boolean":
                filter_value = str(query_params.get(param_key, "")).strip().lower()
                if not filter_value:
                    continue
                expected = filter_value in {"1", "true", "on", "yes"}
                if bool(value) != expected:
                    ok = False
                    break
        if ok:
            filtered.append(submission)
    return filtered


def encode_cursor(created_at: datetime, submission_id: str) -> str:
    value = f"{ensure_aware(created_at).isoformat()}|{submission_id}"
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")


def decode_cursor(cursor: str) -> tuple[datetime, str] | None:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("utf-8")).decode("utf-8")
        created_at_raw, submission_id = raw.split("|", 1)
        created_at = datetime.fromisoformat(created_at_raw)
        return ensure_aware(created_at), submission_id
    except Exception:
        return None


def csv_headers_and_rows(
    fields: list[dict[str, Any]],
    submissions: list[dict[str, Any]],
    file_names: dict[str, str],
) -> tuple[list[str], list[list[str]]]:
    flat = flatten_fields(fields)
    max_lengths: dict[str, int] = {}

    for field in flat:
        if field.get("is_array"):
            fk = field["flat_key"]
            max_len = 1
            for submission in submissions:
                value = get_nested_value(submission.get("data_json", {}), fk)
                if isinstance(value, list):
                    max_len = max(max_len, len(value))
            max_lengths[fk] = max_len

    headers: list[str] = []
    for field in flat:
        fk = field["flat_key"]
        base = field["flat_label"]
        if field.get("is_array"):
            for idx in range(max_lengths.get(fk, 1)):
                headers.append(f"{base}_{idx}")
        else:
            headers.append(base)

    rows: list[list[str]] = []
    for submission in submissions:
        data = submission.get("data_json", {})
        row: list[str] = []
        for field in flat:
            fk = field["flat_key"]
            value = get_nested_value(data, fk)
            is_file = field["type"] == "file"
            if field.get("type") == "group" and field.get("is_array"):
                row.append(format_array_group_value(value, field.get("children", [])))
            elif field.get("is_array"):
                items = value if isinstance(value, list) else []
                max_len = max_lengths.get(fk, 1)
                for idx in range(max_len):
                    item = items[idx] if idx < len(items) else None
                    row.append(value_to_text(item, file_names, is_file))
            else:
                row.append(value_to_text(value, file_names, is_file))
        rows.append(row)

    return headers, rows


@app.post("/admin/forms", response_class=HTMLResponse, tags=["admin"])
async def create_form(request: Request, _: Any = Depends(admin_guard)) -> HTMLResponse:
    form_data = await request.form()
    name = str(form_data.get("name", "")).strip()
    description = str(form_data.get("description", "")).strip()
    fields_json = str(form_data.get("fields_json", ""))

    fields, errors = parse_fields_json(fields_json)
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
                "errors": errors,
            },
        )

    schema, field_order = schema_from_fields(fields)
    form_id = new_ulid()
    public_id = new_short_id()
    now = now_utc()
    STORAGE.forms.create_form(
        {
            "id": form_id,
            "public_id": public_id,
            "name": name,
            "description": description,
            "status": "inactive",
            "schema_json": schema,
            "field_order": field_order,
            "created_at": now,
            "updated_at": now,
        }
    )
    return RedirectResponse(f"/admin/forms/{form_id}", status_code=303)


@app.get("/admin/forms/{form_id}", response_class=HTMLResponse, tags=["admin"])
async def edit_form(request: Request, form_id: str, _: Any = Depends(admin_guard)) -> HTMLResponse:
    form = STORAGE.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    return templates.TemplateResponse(
        "admin_form_builder.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "fields_json": dumps_json(fields),
            "errors": [],
        },
    )


@app.post("/admin/forms/{form_id}", response_class=HTMLResponse, tags=["admin"])
async def update_form(request: Request, form_id: str, _: Any = Depends(admin_guard)) -> HTMLResponse:
    form = STORAGE.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")

    form_data = await request.form()
    name = str(form_data.get("name", "")).strip()
    description = str(form_data.get("description", "")).strip()
    fields_json = str(form_data.get("fields_json", ""))

    fields, errors = parse_fields_json(fields_json)
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
                "errors": errors,
            },
        )

    schema, field_order = schema_from_fields(fields)
    updated = STORAGE.forms.update_form(
        form_id,
        {
            "name": name,
            "description": description,
            "schema_json": schema,
            "field_order": field_order,
            "updated_at": now_utc(),
        },
    )
    return RedirectResponse(f"/admin/forms/{updated['id']}", status_code=303)


@app.post("/admin/forms/{form_id}/publish", tags=["admin"])
async def publish_form(form_id: str, _: Any = Depends(admin_guard)) -> RedirectResponse:
    STORAGE.forms.set_status(form_id, "active")
    return RedirectResponse("/admin/forms", status_code=303)


@app.post("/admin/forms/{form_id}/stop", tags=["admin"])
async def stop_form(form_id: str, _: Any = Depends(admin_guard)) -> RedirectResponse:
    STORAGE.forms.set_status(form_id, "inactive")
    return RedirectResponse("/admin/forms", status_code=303)


@app.get("/f/{public_id}", response_class=HTMLResponse, tags=["public"])
async def public_form(request: Request, public_id: str) -> HTMLResponse:
    form = STORAGE.forms.get_form_by_public_id(public_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
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


async def save_upload(file_obj, form_id: str) -> str:
    file_id = new_ulid()
    destination = SETTINGS.upload_dir / file_id
    content = await file_obj.read()
    if SETTINGS.upload_max_bytes is not None and len(content) > SETTINGS.upload_max_bytes:
        raise HTTPException(status_code=400, detail="ファイルサイズが上限を超えています")
    destination.write_bytes(content)
    STORAGE.files.create_file(
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


@app.post("/f/{public_id}", response_class=HTMLResponse, tags=["public"])
async def submit_form(request: Request, public_id: str) -> HTMLResponse:
    form = STORAGE.forms.get_form_by_public_id(public_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    if form.get("status") != "active":
        return templates.TemplateResponse(
            "form_public.html",
            {
                "request": request,
                "form": form,
                "fields": fields_from_schema(form["schema_json"], form.get("field_order", [])),
                "errors": ["このフォームは停止中です"],
                "inactive": True,
            },
        )

    form_data = await request.form()
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
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
                            rest = k[len(form_prefix):]
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
                            file_ids.append(await save_upload(upload, form["id"]))
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
                        target[key] = await save_upload(upload, form["id"])
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
    if errors:
        messages = [f"{error.message}" for error in errors]
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

    STORAGE.submissions.create_submission(
        {
            "id": new_ulid(),
            "form_id": form["id"],
            "data_json": submission,
            "created_at": now_utc(),
        }
    )

    return templates.TemplateResponse(
        "submission_done.html",
        {"request": request, "form": form},
    )


@app.get("/admin/forms/{form_id}/submissions", response_class=HTMLResponse, tags=["admin"])
async def list_submissions(request: Request, form_id: str, _: Any = Depends(admin_guard)) -> HTMLResponse:
    form = STORAGE.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")

    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    submissions = STORAGE.submissions.list_submissions(form_id)

    filtered = apply_filters(submissions, fields, dict(request.query_params))

    page = int(request.query_params.get("page", 1))
    page_size = int(request.query_params.get("page_size", 50))
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]

    flat_fields = flatten_fields(fields)

    file_ids = collect_file_ids(page_items, fields)
    file_names = resolve_file_names(STORAGE.files, file_ids)

    display_rows = []
    for item in page_items:
        row_values = []
        data = item.get("data_json", {})
        for ff in flat_fields:
            fk = ff["flat_key"]
            value = get_nested_value(data, fk)
            if ff.get("type") == "group" and ff.get("is_array"):
                row_values.append(format_array_group_value(value, ff.get("children", [])))
            else:
                row_values.append(
                    value_to_text(value, file_names, ff["type"] == "file")
                )
        display_rows.append(
            {
                "id": item["id"],
                "created_at": item["created_at"],
                "values": row_values,
            }
        )

    total_pages = max(1, (total + page_size - 1) // page_size)

    return templates.TemplateResponse(
        "submissions.html",
        {
            "request": request,
            "form": form,
            "fields": fields,
            "flat_fields": flat_fields,
            "rows": display_rows,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "query": dict(request.query_params),
        },
    )


@app.post("/admin/forms/{form_id}/submissions/{submission_id}/delete", tags=["admin"])
async def delete_submission(
    form_id: str, submission_id: str, _: Any = Depends(admin_guard)
) -> RedirectResponse:
    STORAGE.submissions.delete_submission(submission_id)
    return RedirectResponse(f"/admin/forms/{form_id}/submissions", status_code=303)


@app.get("/admin/forms/{form_id}/export", tags=["admin"])
async def export_submissions(request: Request, form_id: str, _: Any = Depends(admin_guard)) -> PlainTextResponse:
    form = STORAGE.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")

    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    submissions = STORAGE.submissions.list_submissions(form_id)
    filtered = apply_filters(submissions, fields, dict(request.query_params))

    file_ids = collect_file_ids(filtered, fields)
    file_names = resolve_file_names(STORAGE.files, file_ids)

    headers, rows = csv_headers_and_rows(fields, filtered, file_names)

    fmt = request.query_params.get("format", "csv")
    delimiter = "," if fmt == "csv" else "\t"

    output = io.StringIO()
    writer = csv.writer(output, delimiter=delimiter)
    writer.writerow(headers)
    writer.writerows(rows)

    content_type = "text/csv" if fmt == "csv" else "text/tab-separated-values"
    filename = f"submissions.{fmt}"
    return PlainTextResponse(
        output.getvalue(),
        media_type=content_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/files/{file_id}", tags=["public"])
async def download_file(file_id: str) -> FileResponse:
    file_meta = STORAGE.files.get_file(file_id)
    if not file_meta:
        raise HTTPException(status_code=404, detail="ファイルが見つかりません")
    path = Path(file_meta["stored_path"]).resolve()
    if SETTINGS.upload_dir.resolve() not in path.parents:
        raise HTTPException(status_code=400, detail="不正なファイルパスです")
    return FileResponse(path, filename=file_meta.get("original_name") or file_id)


@app.get("/healthz", tags=["system"])
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/forms", tags=["api/forms"])
async def api_list_forms() -> JSONResponse:
    forms = STORAGE.forms.list_forms()
    return JSONResponse([sanitize_form_output(form) for form in forms])


@app.post("/api/forms", tags=["api/forms"])
async def api_create_form(request: Request) -> JSONResponse:
    payload = await request.json()
    name = str(payload.get("name", "")).strip()
    description = str(payload.get("description", "")).strip()
    schema = payload.get("schema_json") or {}
    if not isinstance(schema, dict):
        raise HTTPException(status_code=400, detail="schema_jsonが不正です")
    if not name:
        raise HTTPException(status_code=400, detail="nameは必須です")
    field_order = normalize_field_order(schema, payload.get("field_order"))

    form_id = new_ulid()
    public_id = new_short_id()
    now = now_utc()
    STORAGE.forms.create_form(
        {
            "id": form_id,
            "public_id": public_id,
            "name": name,
            "description": description,
            "status": payload.get("status", "inactive"),
            "schema_json": schema,
            "field_order": field_order,
            "created_at": now,
            "updated_at": now,
        }
    )
    form = STORAGE.forms.get_form(form_id)
    return JSONResponse(sanitize_form_output(form or {}))


@app.put("/api/forms/{form_id}", tags=["api/forms"])
async def api_update_form(form_id: str, request: Request) -> JSONResponse:
    form = STORAGE.forms.get_form(form_id)
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
        updates["field_order"] = normalize_field_order(schema, payload.get("field_order"))
    if "status" in payload:
        updates["status"] = str(payload.get("status") or "inactive")
    updates["updated_at"] = now_utc()
    updated = STORAGE.forms.update_form(form_id, updates)
    return JSONResponse(sanitize_form_output(updated))


@app.post("/api/public/forms/{public_id}/submissions", tags=["api/submissions"])
async def api_submit_form(public_id: str, request: Request) -> JSONResponse:
    form = STORAGE.forms.get_form_by_public_id(public_id)
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
    if errors:
        raise HTTPException(status_code=400, detail="バリデーションに失敗しました")

    submission_id = new_ulid()
    created_at = now_utc()
    STORAGE.submissions.create_submission(
        {"id": submission_id, "form_id": form["id"], "data_json": data, "created_at": created_at}
    )
    return JSONResponse({"submission_id": submission_id, "created_at": to_iso(created_at)})


@app.get("/api/forms/{form_id}/submissions", tags=["api/submissions"])
async def api_list_submissions(request: Request, form_id: str) -> JSONResponse:
    form = STORAGE.forms.get_form(form_id)
    if not form:
        raise HTTPException(status_code=404, detail="フォームが見つかりません")
    fields = fields_from_schema(form["schema_json"], form.get("field_order", []))
    submissions = STORAGE.submissions.list_submissions(form_id)

    filtered = apply_filters(submissions, fields, dict(request.query_params))
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
                    ensure_aware(item["created_at"]) == cursor_dt and item["id"] < cursor_id
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
        }
        for item in page_items
    ]
    headers: dict[str, str] = {}
    if len(page_items) == limit:
        last = page_items[-1]
        headers["X-Next-Cursor"] = encode_cursor(last["created_at"], last["id"])
    return JSONResponse(response_items, headers=headers)


def run_server(host: str | None, port: int | None) -> None:
    import uvicorn

    resolved_host = host or SETTINGS.host
    resolved_port = port if port is not None else SETTINGS.port
    uvicorn.run(app, host=resolved_host, port=resolved_port)


@cli.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    host: str | None = typer.Option(None, help="バインドするアドレス"),
    port: int | None = typer.Option(None, help="バインドするポート"),
) -> None:
    ctx.obj = {"host": host, "port": port}
    if ctx.invoked_subcommand is None:
        run_server(host, port)


@cli.command()
def run(
    ctx: typer.Context,
    host: str | None = typer.Option(None, help="バインドするアドレス"),
    port: int | None = typer.Option(None, help="バインドするポート"),
) -> None:
    base = ctx.obj or {}
    resolved_host = host or base.get("host")
    resolved_port = port if port is not None else base.get("port")
    run_server(resolved_host, resolved_port)


if __name__ == "__main__":
    cli()
