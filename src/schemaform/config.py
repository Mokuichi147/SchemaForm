from __future__ import annotations

import os
import re
import sys
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

def _get_base_dir() -> Path:
    # PyInstaller で --onefile ビルドした場合、実行時に sys._MEIPASS へ展開される
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent.parent.parent


BASE_DIR = _get_base_dir()


class Settings:
    def __init__(self) -> None:
        self.storage_backend = os.getenv("SCHEMAFORM_STORAGE_BACKEND", "sqlite").lower()
        self.sqlite_path = Path(os.getenv("SCHEMAFORM_SQLITE_PATH", "./data/app.db"))
        self.json_path = Path(
            os.getenv("SCHEMAFORM_JSON_PATH", "./data/jsonstore.json")
        )
        self.upload_dir = Path(os.getenv("SCHEMAFORM_UPLOAD_DIR", "./data/uploads"))
        max_bytes = os.getenv("SCHEMAFORM_UPLOAD_MAX_BYTES")
        self.upload_max_bytes = int(max_bytes) if max_bytes else None
        self.solo = os.getenv("SCHEMAFORM_SOLO", "").lower() in ("1", "true", "yes")
        db_value = os.getenv("SCHEMAFORM_USER_PERMISSION_DB")
        self.user_permission_db: str = db_value or "./data/users.db"
        self.user_permission_secret = Path(
            os.getenv("SCHEMAFORM_USER_PERMISSION_SECRET", "./data/users.secret")
        )
        self.file_url_secret = Path(
            os.getenv("SCHEMAFORM_FILE_URL_SECRET", "./data/file_url.secret")
        )
        try:
            self.file_url_ttl_seconds = int(
                os.getenv("SCHEMAFORM_FILE_URL_TTL_SECONDS", "86400")
            )
        except ValueError:
            self.file_url_ttl_seconds = 86400
        self.user_permission_admin_group = os.getenv(
            "SCHEMAFORM_USER_PERMISSION_ADMIN_GROUP", "admins"
        )
        self.user_permission_token_cookie = os.getenv(
            "SCHEMAFORM_USER_PERMISSION_TOKEN_COOKIE", "sf_token"
        )
        try:
            self.user_permission_token_hours = int(
                os.getenv("SCHEMAFORM_USER_PERMISSION_TOKEN_HOURS", "24")
            )
        except ValueError:
            self.user_permission_token_hours = 24
        self.allow_signup = os.getenv("SCHEMAFORM_ALLOW_SIGNUP", "true").lower() in (
            "1",
            "true",
            "yes",
        )
        try:
            self.password_min_length = int(
                os.getenv("SCHEMAFORM_PASSWORD_MIN_LENGTH", "8")
            )
        except ValueError:
            self.password_min_length = 8
        if self.password_min_length < 1:
            self.password_min_length = 1
        self.host = os.getenv("SCHEMAFORM_HOST", "0.0.0.0")
        port_value = os.getenv("SCHEMAFORM_PORT", "8000")
        try:
            self.port = int(port_value)
        except ValueError:
            self.port = 8000


def ensure_dirs(settings: Settings) -> None:
    settings.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    settings.json_path.parent.mkdir(parents=True, exist_ok=True)
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
