from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import markupsafe
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from schemaform.auth import LoginRequired, get_auth_provider
from schemaform.config import BASE_DIR, Settings, ensure_dirs
from schemaform.file_formats import file_accept_for_constraints
from schemaform.routes.admin import router as admin_router
from schemaform.routes.admin_groups import router as admin_groups_router
from schemaform.routes.api import router as api_router
from schemaform.routes.auth import router as auth_router
from schemaform.routes.public import router as public_router
from schemaform.routes.submissions import router as submissions_router
from schemaform.routes.user import router as user_router
from schemaform.storage import init_storage


def _tojson_attr(value: Any) -> markupsafe.Markup:
    """JSON文字列をHTML属性に安全に埋め込めるようエスケープする。"""
    return markupsafe.Markup(markupsafe.escape(json.dumps(value, ensure_ascii=False)))


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


def field_file_accept(field: dict[str, Any]) -> str:
    if field.get("type") != "file":
        return ""
    return file_accept_for_constraints(
        file_format=field.get("format"),
        allowed_extensions=field.get("allowed_extensions"),
    )


def format_dt(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone().strftime("%Y-%m-%d %H:%M")
    return str(value or "")


def iso_dt(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    return ""


def build_query(base: dict[str, Any], **overrides: Any) -> str:
    params = {k: v for k, v in base.items() if v not in (None, "")}
    for key, value in overrides.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = str(value)
    return urlencode(params, doseq=True)


def get_current_user(request: Request) -> dict[str, Any] | None:
    """テンプレートから request.state.current_user を安全に取り出すヘルパー。"""
    return getattr(request.state, "current_user", None)


def get_auth_enabled(request: Request) -> bool:
    """認証が有効かどうか（テンプレート用）。"""
    settings: Settings | None = getattr(request.app.state, "settings", None)
    return bool(settings and not settings.solo)


def get_signup_enabled(request: Request) -> bool:
    """セルフサインアップが有効かどうか（テンプレート用）。"""
    settings: Settings | None = getattr(request.app.state, "settings", None)
    auth = getattr(request.app.state, "auth_provider", None)
    if settings is None or settings.solo or not settings.allow_signup:
        return False
    return bool(getattr(auth, "signup_supported", False))


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    ensure_dirs(settings)
    storage = init_storage(settings)
    auth = get_auth_provider(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await auth.connect()
        bootstrap = getattr(auth, "bootstrap_admin_if_needed", None)
        if bootstrap is not None:
            created = await bootstrap()
            if created is not None:
                username, password = created
                print(
                    "=" * 60
                    + f"\n初回起動: 管理者ユーザーを自動作成しました\n"
                    + f"  username: {username}\n"
                    + f"  password: {password}\n"
                    + "このパスワードは再表示されません。必ず控えてください。\n"
                    + "=" * 60,
                    flush=True,
                )
        try:
            yield
        finally:
            await auth.close()

    app = FastAPI(
        lifespan=lifespan,
        openapi_tags=[
            {"name": "admin", "description": "管理画面（HTML）"},
            {"name": "user", "description": "利用者画面（HTML）"},
            {"name": "public", "description": "公開フォーム（HTML）"},
            {"name": "auth", "description": "認証"},
            {"name": "api/forms", "description": "REST API: フォーム"},
            {"name": "api/submissions", "description": "REST API: 送信"},
            {"name": "system", "description": "システム"},
        ],
    )

    app.state.storage = storage
    app.state.settings = settings
    app.state.auth_provider = auth

    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    app.state.templates = templates

    templates.env.filters["tojson_attr"] = _tojson_attr
    templates.env.globals["field_input_type"] = field_input_type
    templates.env.globals["field_picker"] = field_picker
    templates.env.globals["field_file_accept"] = field_file_accept
    templates.env.globals["format_dt"] = format_dt
    templates.env.globals["iso_dt"] = iso_dt
    templates.env.globals["build_query"] = build_query
    templates.env.globals["get_current_user"] = get_current_user
    templates.env.globals["get_auth_enabled"] = get_auth_enabled
    templates.env.globals["get_signup_enabled"] = get_signup_enabled

    @app.middleware("http")
    async def load_current_user_middleware(request: Request, call_next):
        await auth.load_current_user(request)
        return await call_next(request)

    @app.exception_handler(LoginRequired)
    async def login_required_handler(request: Request, exc: LoginRequired):
        next_q = urlencode({"next": exc.next_path}) if exc.next_path else ""
        target = f"/login?{next_q}" if next_q else "/login"
        return RedirectResponse(target, status_code=303)

    app.include_router(auth_router)
    app.include_router(admin_router)
    app.include_router(admin_groups_router)
    app.include_router(user_router)
    app.include_router(public_router)
    app.include_router(submissions_router)
    app.include_router(api_router)

    return app
