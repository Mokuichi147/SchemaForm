from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator

from schemaform.calculated import _apply_aggregate, _collect_numeric_values
from schemaform.fields import flatten_fields, get_nested_value

NUMERIC_TYPES = {"number", "integer", "calculated"}
TRUTHY_STRINGS = {"true", "1", "yes", "on"}


def _format_num(value: float) -> str:
    """数値を表示用文字列に整形する（整数値は小数点なし、それ以外は小数2桁）。"""
    rounded = round(float(value), 2)
    if rounded == int(rounded):
        return str(int(rounded))
    return f"{rounded:g}"


def _format_stat(value: float | None) -> str:
    return "—" if value is None else _format_num(value)


def _iter_field_values(
    submissions: list[dict[str, Any]], flat_key: str
) -> Iterator[Any]:
    for submission in submissions:
        yield get_nested_value(submission.get("data_json", {}), flat_key)


def _distinct_submissions(
    submissions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """配列グループ展開で重複した送信を id 単位に一意化する。"""
    seen: set[Any] = set()
    result: list[dict[str, Any]] = []
    for submission in submissions:
        sid = submission.get("id")
        if sid in seen:
            continue
        seen.add(sid)
        result.append(submission)
    return result


def build_histogram(
    values: list[float], max_bins: int = 10, is_integer: bool = False
) -> dict[str, Any]:
    """数値リストを等幅ビンに集計する。

    値が0/1個・全同値の場合は単一ビンにフォールバックしてゼロ除算を避ける。
    整数かつレンジが狭い場合は値ごとのビンにする。

    各ビンには `ranges`（[下限, 上限] の組）と `mode`（"range" は半開区間で最終ビンのみ
    上限を含む、"exact" は値一致）を併せて返し、クリック時のドリルダウン照合に使う。
    """
    if not values:
        return {"labels": [], "counts": [], "ranges": [], "mode": "range"}

    low = min(values)
    high = max(values)
    if low == high:
        return {
            "labels": [_format_num(low)],
            "counts": [len(values)],
            "ranges": [[low, high]],
            "mode": "exact",
        }

    if (
        is_integer
        and float(low).is_integer()
        and float(high).is_integer()
        and (high - low) <= max_bins - 1
    ):
        ilow, ihigh = int(low), int(high)
        labels = [str(v) for v in range(ilow, ihigh + 1)]
        counts = [0] * len(labels)
        for value in values:
            counts[int(round(value)) - ilow] += 1
        ranges = [[v, v] for v in range(ilow, ihigh + 1)]
        return {"labels": labels, "counts": counts, "ranges": ranges, "mode": "exact"}

    bins = max_bins
    width = (high - low) / bins
    edges = [low + width * i for i in range(bins + 1)]
    counts = [0] * bins
    for value in values:
        index = int((value - low) / width)
        if index >= bins:
            index = bins - 1
        elif index < 0:
            index = 0
        counts[index] += 1
    labels = [
        f"{_format_num(edges[i])}–{_format_num(edges[i + 1])}" for i in range(bins)
    ]
    ranges = [[edges[i], edges[i + 1]] for i in range(bins)]
    return {"labels": labels, "counts": counts, "ranges": ranges, "mode": "range"}


def _distribution_enum(
    field: dict[str, Any], submissions: list[dict[str, Any]], flat_key: str
) -> dict[str, Any]:
    order: list[str] = [str(v) for v in (field.get("enum") or [])]
    counts: dict[str, int] = {label: 0 for label in order}
    seen = set(order)
    total = 0
    for value in _iter_field_values(submissions, flat_key):
        items = value if isinstance(value, list) else [value]
        for item in items:
            if item is None or item == "":
                continue
            key = str(item)
            if key not in seen:
                order.append(key)
                seen.add(key)
                counts[key] = 0
            counts[key] += 1
            total += 1
    return {
        "key": flat_key,
        "label": field["flat_label"],
        "kind": "distribution",
        "is_enum": True,
        "is_array": bool(field.get("is_array")),
        "unique_items": bool(field.get("unique_items")),
        "supports_correct": True,
        "labels": order,
        "counts": [counts[label] for label in order],
        "total": total,
    }


def _distribution_boolean(
    field: dict[str, Any], submissions: list[dict[str, Any]], flat_key: str
) -> dict[str, Any]:
    true_count = 0
    false_count = 0
    total = 0
    for value in _iter_field_values(submissions, flat_key):
        items = value if isinstance(value, list) else [value]
        for item in items:
            if item is None or item == "":
                continue
            total += 1
            truthy = item is True or (
                isinstance(item, str) and item.lower() in TRUTHY_STRINGS
            )
            if truthy:
                true_count += 1
            else:
                false_count += 1
    return {
        "key": flat_key,
        "label": field["flat_label"],
        "kind": "distribution",
        "is_enum": False,
        "supports_correct": False,
        "labels": ["true", "false"],
        "counts": [true_count, false_count],
        "total": total,
    }


def _numeric(
    field: dict[str, Any], submissions: list[dict[str, Any]], flat_key: str
) -> dict[str, Any]:
    values: list[float] = []
    for value in _iter_field_values(submissions, flat_key):
        values.extend(_collect_numeric_values(value))
    stats = {
        "count": str(int(_apply_aggregate("count", values))),
        "sum": _format_stat(_apply_aggregate("sum", values)),
        "avg": _format_stat(_apply_aggregate("avg", values)),
        "min": _format_stat(_apply_aggregate("min", values)),
        "max": _format_stat(_apply_aggregate("max", values)),
    }
    histogram = build_histogram(values, is_integer=field.get("type") == "integer")
    return {
        "key": flat_key,
        "label": field["flat_label"],
        "kind": "numeric",
        "stats": stats,
        "histogram": histogram,
    }


def to_utc_iso(value: Any) -> str | None:
    """datetime/ISO文字列を UTC・秒精度の ISO 文字列にする。naive はUTCとみなす。

    ブラウザ側で各自のローカル時刻に変換して粒度バケットを作るため、絶対時刻
    （UTC）で受け渡す。送信一覧の日時表示と同じ基準。"""
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        aware = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    else:
        return None
    return aware.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _timeseries_timestamps(submissions: list[dict[str, Any]]) -> list[str]:
    """件数推移グラフ用に、各送信の送信日時（UTC・秒精度のISO文字列）を返す。

    横軸の粒度（秒・分・時・日・週・月・年）は、送信日時の間隔に応じてクライアント
    側で自動選択し、手動でも切り替えられるようにする。"""
    out: list[str] = []
    for submission in submissions:
        iso = to_utc_iso(submission.get("created_at"))
        if iso is not None:
            out.append(iso)
    out.sort()
    return out


def aggregate_submissions(
    fields: list[dict[str, Any]], submissions: list[dict[str, Any]]
) -> dict[str, Any]:
    """フィールド定義と送信一覧から、表示用の集計結果を組み立てる。

    submissions は配列グループ展開済み（送信一覧と同じ）を想定する。件数・時系列は
    送信 id 単位に一意化して実際の送信数を反映する。
    """
    flat_fields = flatten_fields(fields, expand_rows_for_group_arrays=True)
    aggregations: list[dict[str, Any]] = []
    for field in flat_fields:
        field_type = field.get("type")
        flat_key = field["flat_key"]
        if field_type == "enum":
            aggregations.append(_distribution_enum(field, submissions, flat_key))
        elif field_type == "boolean":
            aggregations.append(_distribution_boolean(field, submissions, flat_key))
        elif field_type in NUMERIC_TYPES:
            aggregations.append(_numeric(field, submissions, flat_key))

    distinct = _distinct_submissions(submissions)
    return {
        "aggregations": aggregations,
        "timeseries_timestamps": _timeseries_timestamps(distinct),
        "total_submissions": len(distinct),
    }
