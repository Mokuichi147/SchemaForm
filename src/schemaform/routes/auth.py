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


@router.post("/logout", tags=["auth"])
async def logout(request: Request) -> RedirectResponse:
    auth = request.app.state.auth_provider
    cookie_name = getattr(auth, "cookie_name", "sf_token")
    response = RedirectResponse("/forms", status_code=303)
    response.delete_cookie(cookie_name, path="/")
    return response
