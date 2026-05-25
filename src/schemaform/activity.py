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


def _truncate(text: str, max_len: int) -> str:
    return text if len(text) <= max_len else text[: max_len - 1] + "…"


def build_form_field_summary(
    fields: list[dict[str, Any]], *, max_fields: int = 4
) -> str:
    """フォームの項目構成を「項目: ラベル / ラベル ...」の短い文字列にまとめる。

    作成・削除ログに付与し、どんな項目を持つフォームだったかを後から確認できるようにする。"""
    labels = [str(f.get("label") or f.get("key") or "").strip() for f in fields]
    labels = [label for label in labels if label]
    total = len(labels)
    if total == 0:
        return ""
    shown = " / ".join(labels[:max_fields])
    if total > max_fields:
        return _truncate(f"項目: {shown} ほか（全{total}項目）", 80)
    return _truncate(f"項目: {shown}", 80)


def build_form_change_detail(
    old: dict[str, Any] | None, updates: dict[str, Any]
) -> str:
    """フォーム更新で変更された箇所を列挙した文字列を作る。

    変更前後の比較で、名称・説明・項目・公開範囲などのどこが変わったかを示す。"""
    if not old:
        return ""
    changes: list[str] = []
    new_name = updates.get("name")
    if new_name is not None and new_name != old.get("name"):
        changes.append(f"名称「{old.get('name') or ''}」→「{new_name}」")
    if "description" in updates and updates["description"] != old.get("description"):
        changes.append("説明を変更")
    if ("schema_json" in updates and updates["schema_json"] != old.get("schema_json")) or (
        "field_order" in updates and updates["field_order"] != old.get("field_order")
    ):
        changes.append("項目を変更")
    if (
        "publish_group_ids" in updates
        and updates["publish_group_ids"] != old.get("publish_group_ids")
    ) or (
        "allow_anonymous" in updates
        and bool(updates["allow_anonymous"]) != bool(old.get("allow_anonymous"))
    ):
        changes.append("公開範囲を変更")
    if "edit_group_ids" in updates and updates["edit_group_ids"] != old.get(
        "edit_group_ids"
    ):
        changes.append("編集範囲を変更")
    webhook_keys = (
        "webhook_url",
        "webhook_on_submit",
        "webhook_on_delete",
        "webhook_on_edit",
    )
    if any(
        key in updates and updates[key] != old.get(key) for key in webhook_keys
    ):
        changes.append("Webhook設定を変更")
    if "allow_view_others" in updates and bool(updates["allow_view_others"]) != bool(
        old.get("allow_view_others")
    ):
        changes.append("他ユーザー送信の閲覧設定を変更")
    if "disallow_edit_submissions" in updates and bool(
        updates["disallow_edit_submissions"]
    ) != bool(old.get("disallow_edit_submissions")):
        changes.append("送信内容の変更可否を変更")
    if not changes:
        return ""
    return _truncate(" / ".join(changes), 100)


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
