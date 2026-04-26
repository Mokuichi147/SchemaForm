from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()


async def admin_guard(request: Request) -> None:
    await request.app.state.auth_provider.require_admin(request)


def _ensure_supported(auth: Any) -> None:
    if not hasattr(auth, "list_groups"):
        raise HTTPException(
            status_code=404, detail="このモードではグループ管理は利用できません"
        )


@router.get(
    "/admin/groups", response_class=HTMLResponse, tags=["admin"], response_model=None
)
async def list_groups(
    request: Request, _: Any = Depends(admin_guard)
) -> HTMLResponse:
    auth = request.app.state.auth_provider
    _ensure_supported(auth)
    user = await auth.require_login(request)
    token = user.get("token", "")
    groups = await auth.list_groups(token)
    storage = request.app.state.storage
    form_creator_ids = set(storage.settings.get_form_creator_groups())
    for g in groups:
        g["can_create_form"] = g["id"] in form_creator_ids
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "admin_groups.html",
        {
            "request": request,
            "groups": groups,
            "errors": request.query_params.getlist("error"),
            "notice": request.query_params.get("notice"),
        },
    )


@router.post("/admin/groups", tags=["admin"], response_model=None)
async def create_group(
    request: Request,
    name: str = Form(""),
    description: str = Form(""),
    _: Any = Depends(admin_guard),
) -> RedirectResponse:
    auth = request.app.state.auth_provider
    _ensure_supported(auth)
    user = await auth.require_login(request)
    token = user.get("token", "")

    name = name.strip()
    if not name:
        return RedirectResponse(
            "/admin/groups?error=グループ名を入力してください", status_code=303
        )

    ok, err = await auth.create_group(name, description.strip(), token)
    if not ok:
        return RedirectResponse(
            f"/admin/groups?error={err or 'グループの作成に失敗しました'}",
            status_code=303,
        )
    return RedirectResponse(
        "/admin/groups?notice=グループを作成しました", status_code=303
    )


@router.get(
    "/admin/groups/{group_id}",
    response_class=HTMLResponse,
    tags=["admin"],
    response_model=None,
)
async def group_detail(
    request: Request, group_id: int, _: Any = Depends(admin_guard)
) -> HTMLResponse:
    auth = request.app.state.auth_provider
    _ensure_supported(auth)
    user = await auth.require_login(request)
    token = user.get("token", "")

    group = await auth.get_group(group_id, token)
    if group is None:
        raise HTTPException(status_code=404, detail="グループが見つかりません")

    storage = request.app.state.storage
    group["can_create_form"] = group_id in set(
        storage.settings.get_form_creator_groups()
    )

    members = await auth.get_group_members(group_id, token)
    member_ids = {m["id"] for m in members}
    all_users = await auth.list_users(token)
    candidates = [u for u in all_users if u["id"] not in member_ids]

    templates = request.app.state.templates
    return templates.TemplateResponse(
        "admin_group_detail.html",
        {
            "request": request,
            "group": group,
            "members": members,
            "candidates": candidates,
            "errors": request.query_params.getlist("error"),
            "notice": request.query_params.get("notice"),
        },
    )


@router.post(
    "/admin/groups/{group_id}", tags=["admin"], response_model=None
)
async def update_group(
    request: Request,
    group_id: int,
    name: str = Form(""),
    description: str = Form(""),
    _: Any = Depends(admin_guard),
) -> RedirectResponse:
    auth = request.app.state.auth_provider
    _ensure_supported(auth)
    user = await auth.require_login(request)
    token = user.get("token", "")

    name = name.strip()
    description = description.strip()
    if not name:
        return RedirectResponse(
            f"/admin/groups/{group_id}?error=グループ名を入力してください",
            status_code=303,
        )

    ok = await auth.update_group(
        group_id, token, name=name, description=description
    )
    if not ok:
        return RedirectResponse(
            f"/admin/groups/{group_id}?error=グループの更新に失敗しました",
            status_code=303,
        )
    return RedirectResponse(
        f"/admin/groups/{group_id}?notice=グループを更新しました",
        status_code=303,
    )


@router.post(
    "/admin/groups/{group_id}/members", tags=["admin"], response_model=None
)
async def add_member(
    request: Request,
    group_id: int,
    user_id: int = Form(...),
    _: Any = Depends(admin_guard),
) -> RedirectResponse:
    auth = request.app.state.auth_provider
    _ensure_supported(auth)
    user = await auth.require_login(request)
    token = user.get("token", "")

    ok = await auth.add_group_member(group_id, user_id, token)
    if not ok:
        return RedirectResponse(
            f"/admin/groups/{group_id}?error=メンバー追加に失敗しました",
            status_code=303,
        )
    return RedirectResponse(
        f"/admin/groups/{group_id}?notice=メンバーを追加しました",
        status_code=303,
    )


@router.post(
    "/admin/groups/{group_id}/permissions",
    tags=["admin"],
    response_model=None,
)
async def update_permissions(
    request: Request,
    group_id: int,
    can_create_form: str = Form(""),
    _: Any = Depends(admin_guard),
) -> RedirectResponse:
    auth = request.app.state.auth_provider
    _ensure_supported(auth)
    user = await auth.require_login(request)
    token = user.get("token", "")

    group = await auth.get_group(group_id, token)
    if group is None:
        raise HTTPException(status_code=404, detail="グループが見つかりません")

    storage = request.app.state.storage
    current = set(storage.settings.get_form_creator_groups())
    enable = can_create_form in ("1", "true", "on", "yes")
    if enable:
        current.add(group_id)
    else:
        current.discard(group_id)
    storage.settings.set_form_creator_groups(sorted(current))

    return RedirectResponse(
        f"/admin/groups/{group_id}?notice=権限を更新しました",
        status_code=303,
    )


@router.post(
    "/admin/groups/{group_id}/members/{user_id}/remove",
    tags=["admin"],
    response_model=None,
)
async def remove_member(
    request: Request,
    group_id: int,
    user_id: int,
    _: Any = Depends(admin_guard),
) -> RedirectResponse:
    auth = request.app.state.auth_provider
    _ensure_supported(auth)
    user = await auth.require_login(request)
    token = user.get("token", "")

    ok = await auth.remove_group_member(group_id, user_id, token)
    if not ok:
        return RedirectResponse(
            f"/admin/groups/{group_id}?error=メンバー削除に失敗しました",
            status_code=303,
        )
    return RedirectResponse(
        f"/admin/groups/{group_id}?notice=メンバーを削除しました",
        status_code=303,
    )
