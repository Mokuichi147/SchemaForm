from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from schemaform.models import Base, FileModel, FormModel, SubmissionModel
from schemaform.utils import dumps_json, loads_json, now_utc


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

    def create_submission(self, submission: dict[str, Any]) -> None:
        with self._Session() as session:
            row = SubmissionModel(
                id=submission["id"],
                form_id=submission["form_id"],
                data_json=dumps_json(submission["data_json"]),
                created_at=submission["created_at"],
            )
            session.add(row)
            session.commit()

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
            "created_at": row.created_at,
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


class SQLiteStorage:
    def __init__(self, db_path: Path) -> None:
        self._engine = create_engine(f"sqlite:///{db_path}", future=True)
        self._Session = sessionmaker(self._engine, expire_on_commit=False)
        Base.metadata.create_all(self._engine)
        self._migrate_add_webhook_columns()
        self.forms = SQLiteFormRepo(self._Session)
        self.submissions = SQLiteSubmissionRepo(self._Session)
        self.files = SQLiteFileRepo(self._Session)

    def _migrate_add_webhook_columns(self) -> None:
        with self._engine.connect() as conn:
            result = conn.execute(text("PRAGMA table_info(forms)"))
            columns = {row[1] for row in result.fetchall()}
            if "webhook_url" not in columns:
                conn.execute(text("ALTER TABLE forms ADD COLUMN webhook_url TEXT"))
            if "webhook_on_submit" not in columns:
                conn.execute(
                    text(
                        "ALTER TABLE forms ADD COLUMN webhook_on_submit INTEGER DEFAULT 0"
                    )
                )
            if "webhook_on_delete" not in columns:
                conn.execute(
                    text(
                        "ALTER TABLE forms ADD COLUMN webhook_on_delete INTEGER DEFAULT 0"
                    )
                )
            conn.commit()
