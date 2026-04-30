from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from filelock import FileLock
from tinydb import Query, TinyDB

from schemaform.utils import now_utc, parse_dt, to_iso


def _normalize_group_ids(value: Any) -> list[int]:
    if not value:
        return []
    result: list[int] = []
    seen: set[int] = set()
    for item in value:
        try:
            gid = int(item)
        except (TypeError, ValueError):
            continue
        if gid in seen:
            continue
        seen.add(gid)
        result.append(gid)
    return sorted(result)


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

    def delete_form(self, form_id: str) -> None:
        with self._db() as db:
            db.table("forms").remove(Query().id == form_id)

    @staticmethod
    def _to_record(form: dict[str, Any], partial: bool = False) -> dict[str, Any]:
        record: dict[str, Any] = {}
        for key, value in form.items():
            if key in {"created_at", "updated_at"}:
                record[key] = to_iso(value) if isinstance(value, datetime) else value
            elif key == "publish_group_ids":
                record[key] = _normalize_group_ids(value)
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
            "webhook_url": record.get("webhook_url", ""),
            "webhook_on_submit": record.get("webhook_on_submit", False),
            "webhook_on_delete": record.get("webhook_on_delete", False),
            "webhook_on_edit": record.get("webhook_on_edit", False),
            "creator_group_id": record.get("creator_group_id"),
            "publish_group_ids": _normalize_group_ids(
                record.get("publish_group_ids")
            ),
            "allow_view_others": bool(record.get("allow_view_others", False)),
            "disallow_edit_submissions": bool(
                record.get(
                    "disallow_edit_submissions",
                    not record.get("allow_edit_submissions", True),
                )
            ),
            "allow_anonymous": bool(record.get("allow_anonymous", False)),
            "created_at": parse_dt(record.get("created_at")),
            "updated_at": parse_dt(record.get("updated_at")),
        }


class JSONSubmissionRepo(JSONRepoBase):
    def list_submissions(self, form_id: str) -> list[dict[str, Any]]:
        with self._db() as db:
            items = db.table("submissions").search(Query().form_id == form_id)
        submissions = [self._from_record(item) for item in items]
        return sorted(submissions, key=lambda x: x["created_at"], reverse=True)

    def get_submission(self, submission_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            items = db.table("submissions").search(Query().id == submission_id)
        return self._from_record(items[0]) if items else None

    def create_submission(self, submission: dict[str, Any]) -> None:
        record = self._to_record(submission)
        with self._db() as db:
            db.table("submissions").insert(record)

    def update_submission(
        self, submission_id: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        with self._db() as db:
            table = db.table("submissions")
            item = table.get(Query().id == submission_id)
            if not item:
                raise KeyError(submission_id)
            if "data_json" in updates:
                item["data_json"] = updates["data_json"]
            if "updated_at" in updates:
                item["updated_at"] = to_iso(updates["updated_at"])
            table.update(item, Query().id == submission_id)
        return self._from_record(item)

    def delete_submission(self, submission_id: str) -> None:
        with self._db() as db:
            db.table("submissions").remove(Query().id == submission_id)

    @staticmethod
    def _to_record(submission: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": submission["id"],
            "form_id": submission["form_id"],
            "data_json": submission["data_json"],
            "user_id": submission.get("user_id"),
            "username": submission.get("username"),
            "created_at": to_iso(submission["created_at"]),
        }

    @staticmethod
    def _from_record(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": record["id"],
            "form_id": record["form_id"],
            "data_json": record.get("data_json", {}),
            "user_id": record.get("user_id"),
            "username": record.get("username"),
            "created_at": parse_dt(record.get("created_at")),
            "updated_at": parse_dt(record.get("updated_at")) if record.get("updated_at") else None,
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


class JSONSettingsRepo(JSONRepoBase):
    def get(self, key: str) -> Any:
        with self._db() as db:
            item = db.table("settings").get(Query().key == key)
        return item.get("value") if item else None

    def set(self, key: str, value: Any) -> None:
        with self._db() as db:
            table = db.table("settings")
            existing = table.get(Query().key == key)
            if existing is None:
                table.insert({"key": key, "value": value})
            else:
                table.update({"value": value}, Query().key == key)

    def get_form_creator_groups(self) -> list[int]:
        value = self.get("form_creator_groups")
        return [int(v) for v in (value or [])]

    def set_form_creator_groups(self, group_ids: list[int]) -> None:
        normalized = sorted({int(g) for g in group_ids})
        self.set("form_creator_groups", normalized)


class JSONStorage:
    def __init__(self, path: Path) -> None:
        self._lock = FileLock(f"{path}.lock")
        self.forms = JSONFormRepo(path, self._lock)
        self.submissions = JSONSubmissionRepo(path, self._lock)
        self.files = JSONFileRepo(path, self._lock)
        self.settings = JSONSettingsRepo(path, self._lock)
