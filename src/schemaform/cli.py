from __future__ import annotations

import asyncio

import typer

from schemaform.app import create_app
from schemaform.config import Settings

cli = typer.Typer(add_completion=False)


def _apply_overrides(
    settings: Settings,
    user_permission_db: str | None,
    user_permission_secret: str | None,
    solo: bool | None = None,
) -> None:
    if user_permission_db is not None and user_permission_db != "":
        settings.user_permission_db = user_permission_db
    if user_permission_secret is not None:
        from pathlib import Path

        settings.user_permission_secret = Path(user_permission_secret)
    if solo is not None:
        settings.solo = solo


@cli.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    host: str | None = typer.Option(None, help="バインドするアドレス"),
    port: int | None = typer.Option(None, help="バインドするポート"),
    user_permission_db: str | None = typer.Option(
        None,
        "--user-permission-db",
        help="user-permission の DBパス or URL",
    ),
    user_permission_secret: str | None = typer.Option(
        None,
        "--user-permission-secret",
        help="user-permission のシークレットキーファイルのパス（ローカルDB使用時）",
    ),
    solo: bool = typer.Option(
        False,
        "--solo",
        help="認証を無効化する個人用モード",
    ),
) -> None:
    ctx.obj = {
        "host": host,
        "port": port,
        "user_permission_db": user_permission_db,
        "user_permission_secret": user_permission_secret,
        "solo": solo,
    }
    if ctx.invoked_subcommand is None:
        run_server(
            host,
            port,
            user_permission_db=user_permission_db,
            user_permission_secret=user_permission_secret,
            solo=solo,
        )


@cli.command()
def run(
    ctx: typer.Context,
    host: str | None = typer.Option(None, help="バインドするアドレス"),
    port: int | None = typer.Option(None, help="バインドするポート"),
    user_permission_db: str | None = typer.Option(
        None,
        "--user-permission-db",
        help="user-permission の DBパス or URL",
    ),
    user_permission_secret: str | None = typer.Option(
        None,
        "--user-permission-secret",
        help="user-permission のシークレットキーファイルのパス（ローカルDB使用時）",
    ),
    solo: bool = typer.Option(
        False,
        "--solo",
        help="認証を無効化する個人用モード",
    ),
) -> None:
    base = ctx.obj or {}
    resolved_host = host or base.get("host")
    resolved_port = port if port is not None else base.get("port")
    resolved_db = user_permission_db or base.get("user_permission_db")
    resolved_secret = user_permission_secret or base.get("user_permission_secret")
    resolved_solo = solo or bool(base.get("solo"))
    run_server(
        resolved_host,
        resolved_port,
        user_permission_db=resolved_db,
        user_permission_secret=resolved_secret,
        solo=resolved_solo,
    )


def run_server(
    host: str | None,
    port: int | None,
    user_permission_db: str | None = None,
    user_permission_secret: str | None = None,
    solo: bool = False,
) -> None:
    import uvicorn

    settings = Settings()
    _apply_overrides(
        settings, user_permission_db, user_permission_secret, solo=solo or None
    )
    app = create_app(settings)
    resolved_host = host or settings.host
    resolved_port = port if port is not None else settings.port
    uvicorn.run(app, host=resolved_host, port=resolved_port)


@cli.command("create-admin")
def create_admin(
    ctx: typer.Context,
    username: str = typer.Argument(..., help="ユーザー名"),
    password: str = typer.Option(
        ..., prompt=True, hide_input=True, confirmation_prompt=True, help="パスワード"
    ),
    display_name: str = typer.Option("", help="表示名"),
    group: str | None = typer.Option(
        None, help="管理者グループ名（省略時は Settings の値）"
    ),
    user_permission_db: str | None = typer.Option(
        None, "--user-permission-db", help="user-permission の DBパス or URL"
    ),
    user_permission_secret: str | None = typer.Option(
        None, "--user-permission-secret", help="シークレットキーファイルのパス"
    ),
) -> None:
    """管理者ユーザーを作成し、管理者グループへ追加する。"""
    base = ctx.obj or {}
    settings = Settings()
    _apply_overrides(
        settings,
        user_permission_db or base.get("user_permission_db"),
        user_permission_secret or base.get("user_permission_secret"),
    )

    admin_group = group or settings.user_permission_admin_group
    asyncio.run(_create_admin(settings, username, password, display_name, admin_group))


async def _create_admin(
    settings: Settings,
    username: str,
    password: str,
    display_name: str,
    group_name: str,
) -> None:
    from user_permission import Database

    backend = settings.user_permission_db
    if str(backend).startswith(("http://", "https://")):
        db = Database(backend)
        is_relay = True
    else:
        db = Database(backend, secret=str(settings.user_permission_secret))
        is_relay = False

    await db.connect()
    try:
        if is_relay:
            typer.echo(
                "リレー（HTTP）バックエンドでは create-admin は未対応です。"
                "中央サーバー側で直接作成してください。",
                err=True,
            )
            raise typer.Exit(code=2)

        existing = await db.users.get_by_username(username)
        if existing is None:
            user = await db.users.create(username, password, display_name=display_name)
            typer.echo(f"ユーザーを作成しました: {user.username} (id={user.id})")
        else:
            user = existing
            typer.echo(f"既存ユーザーを使用します: {user.username} (id={user.id})")

        group = await db.groups.get_by_name(group_name)
        if group is None:
            group = await db.groups.create(
                group_name, description="管理者グループ", is_admin=True
            )
            typer.echo(f"グループを作成しました: {group.name} (id={group.id})")
        elif not getattr(group, "is_admin", False):
            updated = await db.groups.update(group.id, is_admin=True)
            if updated is not None:
                group = updated
                typer.echo(
                    f"グループを管理者グループに昇格しました: {group.name} (id={group.id})"
                )

        added = await db.groups.add_user(group.id, user.id)
        if added:
            typer.echo(f"{user.username} を {group.name} に追加しました")
        else:
            typer.echo(f"{user.username} は既に {group.name} に所属しています")
    finally:
        await db.close()
