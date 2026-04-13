from __future__ import annotations

from datetime import timedelta
from typing import Any, Protocol

from fastapi import HTTPException, Request

from schemaform.config import Settings


class LoginRequired(Exception):
    def __init__(self, next_path: str = "/") -> None:
        self.next_path = next_path


class AuthProvider(Protocol):
    async def load_current_user(self, request: Request) -> None: ...

    async def require_admin(self, request: Request) -> None: ...

    async def require_login(self, request: Request) -> dict[str, Any]: ...

    async def login(self, username: str, password: str) -> str | None: ...

    async def connect(self) -> None: ...

    async def close(self) -> None: ...


class NoAuthProvider:
    async def load_current_user(self, request: Request) -> None:
        request.state.current_user = None

    async def require_admin(self, request: Request) -> None:
        return None

    async def require_login(self, request: Request) -> dict[str, Any]:
        return {"id": None, "username": "", "is_admin": True}

    async def login(self, username: str, password: str) -> str | None:
        return None

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None


class UserPermissionAuthProvider:
    """user-permission ライブラリを用いた認証プロバイダ。

    ローカル SQLite / リモートリレーの双方に対応する。
    """

    def __init__(self, settings: Settings) -> None:
        from user_permission import Database

        self._settings = settings
        backend = settings.user_permission_db
        if str(backend).startswith(("http://", "https://")):
            self._db = Database(backend)
            self._is_relay = True
        else:
            self._db = Database(backend, secret=str(settings.user_permission_secret))
            self._is_relay = False
        self._admin_group = settings.user_permission_admin_group
        self._cookie = settings.user_permission_token_cookie
        self._token_hours = settings.user_permission_token_hours

    @property
    def db(self) -> Any:
        return self._db

    @property
    def cookie_name(self) -> str:
        return self._cookie

    @property
    def token_hours(self) -> int:
        return self._token_hours

    async def connect(self) -> None:
        await self._db.connect()

    async def close(self) -> None:
        await self._db.close()

    async def login(self, username: str, password: str) -> str | None:
        expires = timedelta(hours=self._token_hours)
        if self._is_relay:
            return await self._db.users.authenticate(username, password)
        return await self._db.users.authenticate(
            username, password, expires_delta=expires
        )

    async def _verify_and_fetch_user(
        self, token: str
    ) -> tuple[int, str] | None:
        try:
            if self._is_relay:
                user = await self._db.verify_token(token)
                if user is None:
                    return None
                return (user.id, user.username)
            payload = self._db.token_manager.verify_token(token)
            return (int(payload["sub"]), str(payload.get("username", "")))
        except Exception:
            return None

    async def _fetch_groups(self, user_id: int, token: str) -> list[str]:
        try:
            if self._is_relay:
                groups = await self._db.groups.get_user_groups(user_id, token)
            else:
                groups = await self._db.groups.get_user_groups(user_id)
            return [g.name for g in groups]
        except Exception:
            return []

    async def load_current_user(self, request: Request) -> None:
        token = request.cookies.get(self._cookie)
        if not token:
            request.state.current_user = None
            return
        verified = await self._verify_and_fetch_user(token)
        if verified is None:
            request.state.current_user = None
            return
        user_id, username = verified
        groups = await self._fetch_groups(user_id, token)
        request.state.current_user = {
            "id": user_id,
            "username": username,
            "token": token,
            "groups": groups,
            "is_admin": self._admin_group in groups,
        }

    async def require_login(self, request: Request) -> dict[str, Any]:
        user = getattr(request.state, "current_user", None)
        if user is None:
            raise LoginRequired(next_path=_current_path(request))
        return user

    async def require_admin(self, request: Request) -> None:
        user = getattr(request.state, "current_user", None)
        if user is None:
            raise LoginRequired(next_path=_current_path(request))
        if not user.get("is_admin"):
            raise HTTPException(status_code=403, detail="管理者権限が必要です")

    async def bootstrap_admin_if_needed(self) -> tuple[str, str] | None:
        """ローカルDB かつ 管理者グループにユーザーがいない場合のみ自動作成する。"""
        import secrets
        import string

        if self._is_relay:
            return None
        group = await self._db.groups.get_by_name(self._admin_group)
        if group is None:
            group = await self._db.groups.create(
                self._admin_group, description="管理者グループ"
            )
        members = await self._db.groups.get_members(group.id)
        if members:
            return None
        alphabet = string.ascii_letters + string.digits
        password = "".join(secrets.choice(alphabet) for _ in range(16))
        username = "admin"
        existing = await self._db.users.get_by_username(username)
        user = existing or await self._db.users.create(
            username, password, display_name="Administrator"
        )
        await self._db.groups.add_user(group.id, user.id)
        return (username, password)


def _current_path(request: Request) -> str:
    path = request.url.path
    if request.url.query:
        return f"{path}?{request.url.query}"
    return path


def get_auth_provider(settings: Settings) -> AuthProvider:
    if settings.solo:
        return NoAuthProvider()
    return UserPermissionAuthProvider(settings)
