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
        self.auth_mode = os.getenv("AUTH_MODE", "none").lower()
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
