from __future__ import annotations

from typing import Any, Protocol


class FormRepository(Protocol):
    def list_forms(self) -> list[dict[str, Any]]: ...

    def get_form(self, form_id: str) -> dict[str, Any] | None: ...

    def get_form_by_public_id(self, public_id: str) -> dict[str, Any] | None: ...

    def create_form(self, form: dict[str, Any]) -> None: ...

    def update_form(self, form_id: str, updates: dict[str, Any]) -> dict[str, Any]: ...

    def set_status(self, form_id: str, status: str) -> None: ...

    def delete_form(self, form_id: str) -> None: ...


class SubmissionRepository(Protocol):
    def list_submissions(self, form_id: str) -> list[dict[str, Any]]: ...

    def create_submission(self, submission: dict[str, Any]) -> None: ...

    def delete_submission(self, submission_id: str) -> None: ...


class FileRepository(Protocol):
    def create_file(self, file_meta: dict[str, Any]) -> None: ...

    def get_file(self, file_id: str) -> dict[str, Any] | None: ...


class SettingsRepository(Protocol):
    def get(self, key: str) -> Any: ...

    def set(self, key: str, value: Any) -> None: ...

    def get_form_creator_groups(self) -> list[int]: ...

    def set_form_creator_groups(self, group_ids: list[int]) -> None: ...


class ActivityRepository(Protocol):
    def log_activity(self, activity: dict[str, Any]) -> None: ...

    def list_activities(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        actions: list[str] | None = None,
        form_id: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def count_activities(
        self, *, actions: list[str] | None = None, form_id: str | None = None
    ) -> int: ...

    def form_activity_summary(self) -> list[dict[str, Any]]: ...


class Storage(Protocol):
    forms: FormRepository
    submissions: SubmissionRepository
    files: FileRepository
    settings: SettingsRepository
    activities: ActivityRepository
