from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from schemaform.models import Base, FileModel, FormModel, SettingModel, SubmissionModel
from schemaform.utils import dumps_json, loads_json, now_utc


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


class SQLiteFormRepo:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._Session = session_factory

    def list_forms(self) -> list[dict[str, Any]]:
        with self._Session() as session:
            rows = session.query(FormModel).order_by(FormModel.updated_at.desc()).all()
            return [self._to_dict(row) for row in rows]

    def get_form(self, form_id: str) -> dict[str, Any] | None:
        with self._Session() as session:
            row = session.get(FormModel, form_id)
            return self._to_dict(row) if row else None

    def get_form_by_public_id(self, public_id: str) -> dict[str, Any] | None:
        with self._Session() as session:
            row = (
                session.query(FormModel)
                .filter(FormModel.public_id == public_id)
                .first()
            )
            return self._to_dict(row) if row else None

    def create_form(self, form: dict[str, Any]) -> None:
        with self._Session() as session:
            row = FormModel(
                id=form["id"],
                public_id=form["public_id"],
                name=form["name"],
                description=form["description"],
                status=form["status"],
                schema_json=dumps_json(form["schema_json"]),
                field_order=dumps_json(form["field_order"]),
                webhook_url=form.get("webhook_url", ""),
                webhook_on_submit=1 if form.get("webhook_on_submit") else 0,
                webhook_on_delete=1 if form.get("webhook_on_delete") else 0,
                webhook_on_edit=1 if form.get("webhook_on_edit") else 0,
                creator_group_id=form.get("creator_group_id"),
                publish_group_ids=dumps_json(
                    _normalize_group_ids(form.get("publish_group_ids"))
                ),
                allow_view_others=1 if form.get("allow_view_others", True) else 0,
                allow_edit_submissions=1
                if form.get("allow_edit_submissions", True)
                else 0,
                created_at=form["created_at"],
                updated_at=form["updated_at"],
            )
            session.add(row)
            session.commit()

    def update_form(self, form_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        with self._Session() as session:
            row = session.get(FormModel, form_id)
            if not row:
                raise KeyError(form_id)
            for key, value in updates.items():
                if key in {"schema_json", "field_order"}:
                    setattr(row, key, dumps_json(value))
                elif key in {
                    "webhook_on_submit",
                    "webhook_on_delete",
                    "webhook_on_edit",
                    "allow_view_others",
                    "allow_edit_submissions",
                }:
                    setattr(row, key, 1 if value else 0)
                elif key == "publish_group_ids":
                    setattr(row, key, dumps_json(_normalize_group_ids(value)))
                else:
                    setattr(row, key, value)
            session.commit()
            session.refresh(row)
            return self._to_dict(row)

    def set_status(self, form_id: str, status: str) -> None:
        with self._Session() as session:
            row = session.get(FormModel, form_id)
            if not row:
                raise KeyError(form_id)
            row.status = status
            row.updated_at = now_utc()
            session.commit()

    def delete_form(self, form_id: str) -> None:
        with self._Session() as session:
            row = session.get(FormModel, form_id)
            if row:
                session.delete(row)
                session.commit()

    @staticmethod
    def _to_dict(row: FormModel) -> dict[str, Any]:
        return {
            "id": row.id,
            "public_id": row.public_id,
            "name": row.name,
            "description": row.description or "",
            "status": row.status,
            "schema_json": loads_json(row.schema_json) or {},
            "field_order": loads_json(row.field_order) or [],
            "webhook_url": row.webhook_url or "",
            "webhook_on_submit": bool(row.webhook_on_submit),
            "webhook_on_delete": bool(row.webhook_on_delete),
            "webhook_on_edit": bool(row.webhook_on_edit),
            "creator_group_id": row.creator_group_id,
            "publish_group_ids": _normalize_group_ids(
                loads_json(row.publish_group_ids) if row.publish_group_ids else []
            ),
            "allow_view_others": bool(
                row.allow_view_others if row.allow_view_others is not None else 1
            ),
            "allow_edit_submissions": bool(
                row.allow_edit_submissions
                if row.allow_edit_submissions is not None
                else 1
            ),
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }


class SQLiteSubmissionRepo:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._Session = session_factory

    def list_submissions(self, form_id: str) -> list[dict[str, Any]]:
        with self._Session() as session:
            rows = (
                session.query(SubmissionModel)
                .filter(SubmissionModel.form_id == form_id)
                .order_by(SubmissionModel.created_at.desc())
                .all()
            )
            return [self._to_dict(row) for row in rows]

    def get_submission(self, submission_id: str) -> dict[str, Any] | None:
        with self._Session() as session:
            row = session.get(SubmissionModel, submission_id)
            return self._to_dict(row) if row else None

    def create_submission(self, submission: dict[str, Any]) -> None:
        with self._Session() as session:
            row = SubmissionModel(
                id=submission["id"],
                form_id=submission["form_id"],
                data_json=dumps_json(submission["data_json"]),
                user_id=submission.get("user_id"),
                username=submission.get("username"),
                created_at=submission["created_at"],
            )
            session.add(row)
            session.commit()

    def update_submission(
        self, submission_id: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        with self._Session() as session:
            row = session.get(SubmissionModel, submission_id)
            if not row:
                raise KeyError(submission_id)
            if "data_json" in updates:
                row.data_json = dumps_json(updates["data_json"])
            if "updated_at" in updates:
                row.updated_at = updates["updated_at"]
            session.commit()
            session.refresh(row)
            return self._to_dict(row)

    def delete_submission(self, submission_id: str) -> None:
        with self._Session() as session:
            row = session.get(SubmissionModel, submission_id)
            if row:
                session.delete(row)
                session.commit()

    @staticmethod
    def _to_dict(row: SubmissionModel) -> dict[str, Any]:
        return {
            "id": row.id,
            "form_id": row.form_id,
            "data_json": loads_json(row.data_json) or {},
            "user_id": row.user_id,
            "username": row.username,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }


class SQLiteFileRepo:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._Session = session_factory

    def create_file(self, file_meta: dict[str, Any]) -> None:
        with self._Session() as session:
            row = FileModel(
                id=file_meta["id"],
                form_id=file_meta["form_id"],
                original_name=file_meta["original_name"],
                stored_path=file_meta["stored_path"],
                content_type=file_meta["content_type"],
                size=file_meta["size"],
                created_at=file_meta["created_at"],
            )
            session.add(row)
            session.commit()

    def get_file(self, file_id: str) -> dict[str, Any] | None:
        with self._Session() as session:
            row = session.get(FileModel, file_id)
            if not row:
                return None
            return {
                "id": row.id,
                "form_id": row.form_id,
                "original_name": row.original_name,
                "stored_path": row.stored_path,
                "content_type": row.content_type,
                "size": row.size,
                "created_at": row.created_at,
            }


class SQLiteSettingsRepo:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._Session = session_factory

    def get(self, key: str) -> Any:
        with self._Session() as session:
            row = session.get(SettingModel, key)
            if row is None or row.value is None:
                return None
            return loads_json(row.value)

    def set(self, key: str, value: Any) -> None:
        with self._Session() as session:
            row = session.get(SettingModel, key)
            payload = dumps_json(value)
            if row is None:
                session.add(SettingModel(key=key, value=payload))
            else:
                row.value = payload
            session.commit()

    def get_form_creator_groups(self) -> list[int]:
        value = self.get("form_creator_groups")
        return [int(v) for v in (value or [])]

    def set_form_creator_groups(self, group_ids: list[int]) -> None:
        normalized = sorted({int(g) for g in group_ids})
        self.set("form_creator_groups", normalized)


class SQLiteStorage:
    def __init__(self, db_path: Path) -> None:
        self._engine = create_engine(f"sqlite:///{db_path}", future=True)
        self._Session = sessionmaker(self._engine, expire_on_commit=False)
        Base.metadata.create_all(self._engine)
        self._migrate_add_webhook_columns()
        self.forms = SQLiteFormRepo(self._Session)
        self.submissions = SQLiteSubmissionRepo(self._Session)
        self.files = SQLiteFileRepo(self._Session)
        self.settings = SQLiteSettingsRepo(self._Session)

    def _migrate_add_webhook_columns(self) -> None:
        with self._engine.connect() as conn:
            result = conn.execute(text("PRAGMA table_info(forms)"))
            form_columns = {row[1] for row in result.fetchall()}
            if "webhook_url" not in form_columns:
                conn.execute(text("ALTER TABLE forms ADD COLUMN webhook_url TEXT"))
            if "webhook_on_submit" not in form_columns:
                conn.execute(
                    text(
                        "ALTER TABLE forms ADD COLUMN webhook_on_submit INTEGER DEFAULT 0"
                    )
                )
            if "webhook_on_delete" not in form_columns:
                conn.execute(
                    text(
                        "ALTER TABLE forms ADD COLUMN webhook_on_delete INTEGER DEFAULT 0"
                    )
                )
            if "webhook_on_edit" not in form_columns:
                conn.execute(
                    text(
                        "ALTER TABLE forms ADD COLUMN webhook_on_edit INTEGER DEFAULT 0"
                    )
                )
            if "creator_group_id" not in form_columns:
                conn.execute(
                    text("ALTER TABLE forms ADD COLUMN creator_group_id INTEGER")
                )
            if "publish_group_ids" not in form_columns:
                conn.execute(
                    text("ALTER TABLE forms ADD COLUMN publish_group_ids TEXT")
                )
            if "allow_view_others" not in form_columns:
                conn.execute(
                    text(
                        "ALTER TABLE forms ADD COLUMN allow_view_others INTEGER DEFAULT 1"
                    )
                )
            if "allow_edit_submissions" not in form_columns:
                conn.execute(
                    text(
                        "ALTER TABLE forms ADD COLUMN allow_edit_submissions INTEGER DEFAULT 1"
                    )
                )
            result = conn.execute(text("PRAGMA table_info(submissions)"))
            sub_columns = {row[1] for row in result.fetchall()}
            if "updated_at" not in sub_columns:
                conn.execute(
                    text("ALTER TABLE submissions ADD COLUMN updated_at DATETIME")
                )
            if "user_id" not in sub_columns:
                conn.execute(
                    text("ALTER TABLE submissions ADD COLUMN user_id INTEGER")
                )
            if "username" not in sub_columns:
                conn.execute(
                    text("ALTER TABLE submissions ADD COLUMN username VARCHAR")
                )
            conn.commit()
