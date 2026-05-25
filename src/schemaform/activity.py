from __future__ import annotations

from typing import Any, Iterable

from fastapi import Request

from schemaform.utils import new_ulid, now_utc

# 操作ログのアクション種別
FORM_CREATE = "form_create"
FORM_UPDATE = "form_update"
FORM_DELETE = "form_delete"
FORM_PUBLISH = "form_publish"
FORM_STOP = "form_stop"
SUBMISSION_CREATE = "submission_create"
SUBMISSION_UPDATE = "submission_update"
SUBMISSION_DELETE = "submission_delete"
SUBMISSION_IMPORT = "submission_import"

ACTION_LABELS: dict[str, str] = {
    FORM_CREATE: "フォーム作成",
    FORM_UPDATE: "フォーム変更",
    FORM_DELETE: "フォーム削除",
    FORM_PUBLISH: "フォーム公開",
    FORM_STOP: "フォーム停止",
    SUBMISSION_CREATE: "送信",
    SUBMISSION_UPDATE: "送信変更",
    SUBMISSION_DELETE: "送信削除",
    SUBMISSION_IMPORT: "送信インポート",
}

ACTION_STYLES: dict[str, str] = {
    FORM_CREATE: "bg-sky-100 text-sky-800",
    SUBMISSION_CREATE: "bg-emerald-100 text-emerald-800",
    SUBMISSION_IMPORT: "bg-emerald-100 text-emerald-800",
    FORM_UPDATE: "bg-amber-100 text-amber-800",
    SUBMISSION_UPDATE: "bg-amber-100 text-amber-800",
    FORM_DELETE: "bg-rose-100 text-rose-800",
    SUBMISSION_DELETE: "bg-rose-100 text-rose-800",
    FORM_PUBLISH: "bg-indigo-100 text-indigo-800",
    FORM_STOP: "bg-slate-200 text-slate-700",
}

# 絞り込み用のカテゴリ（表示順）。空文字は全件。
ACTIVITY_CATEGORY_OPTIONS: list[tuple[str, str]] = [
    ("", "すべての操作"),
    ("create", "フォーム作成"),
    ("submit", "送信"),
    ("update", "変更"),
    ("delete", "削除"),
    ("status", "公開状態の変更"),
]

_CATEGORY_ACTIONS: dict[str, tuple[str, ...]] = {
    "create": (FORM_CREATE,),
    "submit": (SUBMISSION_CREATE, SUBMISSION_IMPORT),
    "update": (FORM_UPDATE, SUBMISSION_UPDATE),
    "delete": (FORM_DELETE, SUBMISSION_DELETE),
    "status": (FORM_PUBLISH, FORM_STOP),
}


def category_actions(category: str | None) -> list[str] | None:
    """カテゴリ名に対応するアクションのリストを返す。未指定/不明なら None（全件）。"""
    return list(_CATEGORY_ACTIONS.get(category or "", ())) or None


def _scalar_text(value: Any) -> str:
    if isinstance(value, bool):
        return "はい" if value else "いいえ"
    return str(value)


def _preview_value(field_type: str, value: Any) -> str:
    if field_type == "file":
        if isinstance(value, list):
            return f"ファイル{len(value)}件" if value else ""
        return "ファイル" if value else ""
    if isinstance(value, list):
        return ", ".join(_scalar_text(v) for v in value if v not in (None, ""))
    if isinstance(value, dict):
        return ""
    return _scalar_text(value)


def build_submission_preview(
    fields: list[dict[str, Any]],
    data: dict[str, Any] | None,
    *,
    max_fields: int = 3,
    max_len: int = 60,
) -> str:
    """送信データの主要項目を「ラベル=値」の短い文字列にまとめる。

    操作ログの詳細欄に表示し、送信がどんな内容だったかを後から確認できるようにする。
    グループはスキップし、ファイルは件数のみ示す。"""
    if not isinstance(data, dict):
        return ""
    parts: list[str] = []
    for field in fields:
        if len(parts) >= max_fields:
            break
        if field.get("type") == "group":
            continue
        key = field.get("key")
        value = data.get(key)
        if value in (None, "", [], {}):
            continue
        text = _preview_value(field.get("type", ""), value)
        if not text:
            continue
        label = field.get("label") or key
        parts.append(f"{label}={text}")
    preview = " / ".join(parts)
    if len(preview) > max_len:
        preview = preview[: max_len - 1] + "…"
    return preview


def aggregate_form_summary(
    rows: Iterable[tuple[Any, Any, Any, Any]],
) -> list[dict[str, Any]]:
    """(form_id, form_name, action, created_at) の列からフォーム別の集計を作る。

    送信数・変更数・削除数・合計・最終操作日時を集計し、送信数→合計の降順で返す。
    フォーム名は最新の操作時点のものを採用する（リネームに追従）。
    """
    summary: dict[Any, dict[str, Any]] = {}
    for form_id, form_name, action, created_at in rows:
        if form_id is None:
            continue
        entry = summary.get(form_id)
        if entry is None:
            entry = {
                "form_id": form_id,
                "form_name": form_name or "",
                "submissions": 0,
                "updates": 0,
                "deletes": 0,
                "total": 0,
                "last_activity_at": created_at,
            }
            summary[form_id] = entry
        entry["total"] += 1
        if action in (SUBMISSION_CREATE, SUBMISSION_IMPORT):
            entry["submissions"] += 1
        elif action in (FORM_UPDATE, SUBMISSION_UPDATE):
            entry["updates"] += 1
        elif action in (FORM_DELETE, SUBMISSION_DELETE):
            entry["deletes"] += 1
        last = entry["last_activity_at"]
        if created_at is not None and (last is None or created_at >= last):
            entry["last_activity_at"] = created_at
            if form_name:
                entry["form_name"] = form_name
    result = list(summary.values())
    result.sort(key=lambda e: (e["submissions"], e["total"]), reverse=True)
    return result


def log_activity(
    request: Request,
    action: str,
    *,
    form: dict[str, Any] | None = None,
    form_id: str | None = None,
    form_name: str | None = None,
    submission_id: str | None = None,
    detail: str = "",
) -> None:
    """操作ログを 1 件記録する。記録失敗が本来の操作を妨げないよう例外は握り潰す。"""
    try:
        storage = request.app.state.storage
        repo = getattr(storage, "activities", None)
        if repo is None:
            return
        if form is not None:
            form_id = form_id or form.get("id")
            form_name = form_name or form.get("name")
        user = getattr(request.state, "current_user", None)
        if user:
            user_id = user.get("id")
            username = user.get("display_name") or user.get("username") or None
        else:
            user_id = None
            username = None
        repo.log_activity(
            {
                "id": new_ulid(),
                "action": action,
                "form_id": form_id,
                "form_name": form_name,
                "submission_id": submission_id,
                "user_id": user_id,
                "username": username,
                "detail": detail or "",
                "created_at": now_utc(),
            }
        )
    except Exception:
        pass
