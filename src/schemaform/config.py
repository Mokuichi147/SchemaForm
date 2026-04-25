from __future__ import annotations

import os
import re
from pathlib import Path

ALLOWED_TYPES = {
    "string",
    "number",
    "integer",
    "boolean",
    "enum",
    "file",
    "datetime",
    "date",
    "time",
    "group",
    "master",
    "calculated",
}
KEY_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings:
    def __init__(self) -> None:
        self.storage_backend = os.getenv("STORAGE_BACKEND", "sqlite").lower()
        self.sqlite_path = Path(os.getenv("SQLITE_PATH", "./data/app.db"))
        self.json_path = Path(os.getenv("JSON_PATH", "./data/jsonstore.json"))
        self.upload_dir = Path(os.getenv("UPLOAD_DIR", "./data/uploads"))
        max_bytes = os.getenv("UPLOAD_MAX_BYTES")
        self.upload_max_bytes = int(max_bytes) if max_bytes else None
        self.solo = os.getenv("SOLO", "").lower() in ("1", "true", "yes")
        db_value = os.getenv("USER_PERMISSION_DB")
        self.user_permission_db: str = db_value or "./data/users.db"
        self.user_permission_secret = Path(
            os.getenv("USER_PERMISSION_SECRET", "./data/users.secret")
        )
        self.user_permission_admin_group = os.getenv(
            "USER_PERMISSION_ADMIN_GROUP", "admins"
        )
        self.user_permission_token_cookie = os.getenv(
            "USER_PERMISSION_TOKEN_COOKIE", "sf_token"
        )
        try:
            self.user_permission_token_hours = int(
                os.getenv("USER_PERMISSION_TOKEN_HOURS", "24")
            )
        except ValueError:
            self.user_permission_token_hours = 24
        self.allow_signup = os.getenv("ALLOW_SIGNUP", "true").lower() in (
            "1",
            "true",
            "yes",
        )
        self.host = os.getenv("HOST", "0.0.0.0")
        port_value = os.getenv("PORT", "8000")
        try:
            self.port = int(port_value)
        except ValueError:
            self.port = 8000


def ensure_dirs(settings: Settings) -> None:
    settings.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    settings.json_path.parent.mkdir(parents=True, exist_ok=True)
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
