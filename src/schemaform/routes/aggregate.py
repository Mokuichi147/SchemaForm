from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from schemaform.aggregate import (
    DEFAULT_VIEW,
    DRILL_PARAM,
    aggregate_submissions,
    apply_drill,
    available_views,
    collect_enum_fields,
    describe_drill,
    encode_drill,
    master_label_map,
    parse_drill,
    resolve_view,
)
from schemaform.fields import flatten_filter_fields
from schemaform.routes.submissions import (
    build_submission_display_columns,
    build_table_context,
    form_editor_guard,
    gather_filtered_submissions,
    resolve_user_display_map,
    resolve_user_label,
    sort_submissions,
)
from schemaform.scoring import (
    CORRECT_PARAM,
    build_scoring,
    correctness_cells,
    encode_correct_map,
    parse_correct_map,
    score_of,
)

router = APIRouter()

# 集計中心のテンプレートで元データ表に出す行数（全件は「元データ」テンプレートで見る）。
PREVIEW_ROWS = 10
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def _int_param(request: Request, name: str, default: int) -> int:
    try:
        return int(request.query_params.get(name, default))
    except (TypeError, ValueError):
        return default


@router.get("/forms/{form_id}/aggregate", response_class=HTMLResponse, tags=["user"])
async def aggregate_view(
    request: Request, form_id: str, _: Any = Depends(form_editor_guard)
) -> HTMLResponse:
    templates = request.app.state.templates
    storage = request.app.state.storage
    form, fields, filtered, _file_names = await gather_filtered_submissions(
        request, form_id
    )

    views = available_views(fields)
    view = resolve_view(request.query_params.get("view"))
    if view["id"] not in {item["id"] for item in views}:
        view = resolve_view(DEFAULT_VIEW)

    display_columns, master_lookup_by_field = build_submission_display_columns(
        storage, fields
    )
    master_labels = master_label_map(master_lookup_by_field)

    correct_map = parse_correct_map(request.query_params.get(CORRECT_PARAM))
    drill = parse_drill(request.query_params.get(DRILL_PARAM))
    filtered = apply_drill(
        filtered,
        fields,
        drill,
        master_labels=master_labels,
        score_of=lambda item: score_of(item.get("data_json", {}), correct_map),
    )

    result = aggregate_submissions(
        fields, filtered, view=view, master_labels=master_labels
    )
    scoring = (
        build_scoring(collect_enum_fields(fields), filtered, correct_map)
        if "scoring" in view["blocks"]
        else None
    )

    user_display_map = await resolve_user_display_map(request)
    for item in filtered:
        item["_display_username"] = resolve_user_label(item, user_display_map)

    sort = request.query_params.get("sort", "created_at")
    order = request.query_params.get("order", "desc")
    if sort == "score" and correct_map:
        filtered.sort(
            key=lambda item: score_of(item.get("data_json", {}), correct_map),
            reverse=order != "asc",
        )
    else:
        sort_submissions(filtered, sort, order, display_columns, master_lookup_by_field)

    # 集計中心のテンプレートは先頭数件だけ、「元データ」「テスト採点」は全件ページング。
    preview = view["table"] == "preview"
    total = len(filtered)
    if preview:
        page, page_size = 1, PREVIEW_ROWS
    else:
        page_size = max(1, min(_int_param(request, "page_size", DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE))
        page = max(1, _int_param(request, "page", 1))
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    page_items = filtered[(page - 1) * page_size : page * page_size]

    table = build_table_context(
        request,
        fields,
        page_items,
        display_columns,
        master_lookup_by_field,
        user_display_map,
    )
    if correct_map:
        for row, item in zip(table["rows"], page_items):
            row["score"], row["rate"] = correctness_cells(
                item.get("data_json", {}), correct_map
            )

    # query から集計ページ固有のパラメータを除いたものが「フィルター条件」。
    # ダウンロードや条件解除のリンクを組み立てるときの土台にする。
    query = dict(request.query_params)

    return templates.TemplateResponse(
        "aggregate.html",
        {
            "request": request,
            "form": form,
            "query": query,
            "filter_fields": flatten_filter_fields(fields),
            "views": views,
            "view": view,
            "drill": drill,
            "drill_labels": describe_drill(drill, fields),
            "drill_param": DRILL_PARAM,
            "drill_encoded": encode_drill(drill),
            "correct_param": CORRECT_PARAM,
            "correct_map": correct_map,
            "correct_encoded": encode_correct_map(correct_map),
            "scoring": scoring,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "preview": preview,
            "sort": sort,
            "order": order,
            **result,
            **table,
        },
    )
