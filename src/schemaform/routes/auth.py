from __future__ import annotations

from urllib.parse import urlsplit

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()


def _safe_next(candidate: str | None, default: str = "/forms") -> str:
    value = (candidate or "").strip()
    if not value:
        return default
    if not value.startswith("/") or value.startswith("//"):
        return default
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc:
        return default
    if parsed.query:
        return f"{parsed.path}?{parsed.query}"
    return parsed.path


@router.get("/login", response_class=HTMLResponse, tags=["auth"])
async def login_page(request: Request) -> HTMLResponse:
    templates = request.app.state.templates
    next_path = _safe_next(request.query_params.get("next"))
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "next": next_path,
            "errors": [],
            "username": "",
        },
    )


@router.post("/login", tags=["auth"], response_model=None)
async def login(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    next: str = Form("/forms"),
) -> HTMLResponse | RedirectResponse:
    auth = request.app.state.auth_provider
    templates = request.app.state.templates
    next_path = _safe_next(next)

    username = username.strip()
    if not username or not password:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "next": next_path,
                "errors": ["ユーザー名とパスワードを入力してください"],
                "username": username,
            },
            status_code=400,
        )

    token = await auth.login(username, password)
    if not token:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "next": next_path,
                "errors": ["ユーザー名またはパスワードが正しくありません"],
                "username": username,
            },
            status_code=401,
        )

    response = RedirectResponse(next_path, status_code=303)
    response.set_cookie(
        key=auth.cookie_name,
        value=token,
        max_age=auth.token_hours * 3600,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return response


def _signup_enabled(request: Request) -> bool:
    settings = getattr(request.app.state, "settings", None)
    auth = request.app.state.auth_provider
    if settings is None or not getattr(settings, "allow_signup", False):
        return False
    return bool(getattr(auth, "signup_supported", False))


@router.get("/signup", response_class=HTMLResponse, tags=["auth"], response_model=None)
async def signup_page(request: Request) -> HTMLResponse | RedirectResponse:
    if not _signup_enabled(request):
        return RedirectResponse("/login", status_code=303)
    templates = request.app.state.templates
    next_path = _safe_next(request.query_params.get("next"))
    return templates.TemplateResponse(
        "signup.html",
        {
            "request": request,
            "next": next_path,
            "errors": [],
            "username": "",
            "display_name": "",
        },
    )


@router.post("/signup", tags=["auth"], response_model=None)
async def signup(
    request: Request,
    username: str = Form(""),
    display_name: str = Form(""),
    password: str = Form(""),
    password_confirm: str = Form(""),
    next: str = Form("/forms"),
) -> HTMLResponse | RedirectResponse:
    if not _signup_enabled(request):
        return RedirectResponse("/login", status_code=303)
    auth = request.app.state.auth_provider
    templates = request.app.state.templates
    next_path = _safe_next(next)

    username = username.strip()
    display_name = display_name.strip()

    errors: list[str] = []
    if not username:
        errors.append("ユーザー名を入力してください")
    if not password:
        errors.append("パスワードを入力してください")
    if password and len(password) < 8:
        errors.append("パスワードは 8 文字以上にしてください")
    if password and password != password_confirm:
        errors.append("パスワードと確認用パスワードが一致しません")

    if errors:
        return templates.TemplateResponse(
            "signup.html",
            {
                "request": request,
                "next": next_path,
                "errors": errors,
                "username": username,
                "display_name": display_name,
            },
            status_code=400,
        )

    ok, payload = await auth.signup(username, password, display_name=display_name)
    if not ok:
        return templates.TemplateResponse(
            "signup.html",
            {
                "request": request,
                "next": next_path,
                "errors": [payload or "アカウントの作成に失敗しました"],
                "username": username,
                "display_name": display_name,
            },
            status_code=400,
        )

    response = RedirectResponse(next_path, status_code=303)
    response.set_cookie(
        key=auth.cookie_name,
        value=payload,
        max_age=auth.token_hours * 3600,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return response


@router.get("/account", response_class=HTMLResponse, tags=["auth"])
async def account_page(request: Request) -> HTMLResponse:
    auth = request.app.state.auth_provider
    user = await auth.require_login(request)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "account.html",
        {"request": request, "user": user},
    )


@router.get("/account/password", response_class=HTMLResponse, tags=["auth"])
async def password_page(request: Request) -> HTMLResponse:
    auth = request.app.state.auth_provider
    await auth.require_login(request)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "account_password.html",
        {"request": request, "errors": [], "notice": None},
    )


@router.post("/account/password", tags=["auth"], response_model=None)
async def password_update(
    request: Request,
    current_password: str = Form(""),
    new_password: str = Form(""),
    new_password_confirm: str = Form(""),
) -> HTMLResponse:
    auth = request.app.state.auth_provider
    user = await auth.require_login(request)
    templates = request.app.state.templates

    errors: list[str] = []
    if not current_password or not new_password:
        errors.append("現パスワードと新パスワードを入力してください")
    if new_password and new_password != new_password_confirm:
        errors.append("新パスワードと確認用パスワードが一致しません")
    if new_password and len(new_password) < 8:
        errors.append("新パスワードは 8 文字以上にしてください")
    if new_password and new_password == current_password:
        errors.append("新パスワードは現パスワードと異なるものにしてください")

    change_password = getattr(auth, "change_password", None)
    if change_password is None:
        errors.append("パスワード変更はこのモードでは利用できません")

    if errors:
        return templates.TemplateResponse(
            "account_password.html",
            {"request": request, "errors": errors, "notice": None},
            status_code=400,
        )

    ok = await change_password(
        user["id"],
        user["username"],
        user.get("token", ""),
        current_password,
        new_password,
    )
    if not ok:
        return templates.TemplateResponse(
            "account_password.html",
            {
                "request": request,
                "errors": ["現パスワードが正しくありません"],
                "notice": None,
            },
            status_code=401,
        )

    return templates.TemplateResponse(
        "account_password.html",
        {
            "request": request,
            "errors": [],
            "notice": "パスワードを変更しました",
        },
    )


@router.post("/logout", tags=["auth"])
async def logout(request: Request) -> RedirectResponse:
    auth = request.app.state.auth_provider
    cookie_name = getattr(auth, "cookie_name", "sf_token")
    response = RedirectResponse("/forms", status_code=303)
    response.delete_cookie(cookie_name, path="/")
    return response
