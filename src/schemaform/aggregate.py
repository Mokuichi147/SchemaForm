"""送信内容の集計。

集計ページはこのモジュールが組み立てた「ブロック」を並べて表示する。ブロックは
次の3種類しかなく、フィールド型ごとの差はブロックを作る過程に閉じ込めてある。

* ``category`` … ラベルごとの件数（選択肢・真偽・自由記述の頻出値・日付・参照など）
* ``numeric``  … 統計値とヒストグラム（数値・計算フィールド）
* ``crosstab`` … 数値をカテゴリ項目ごとに集計したもの（Group By）

グラフのクリックによる絞り込み（ドリルダウン）は、ブロック内の各項目が持つ
``drill`` 条件をURLに載せて実現する。条件の判定には集計時とまったく同じ値の
正規化関数を使うため、「グラフの件数」と「絞り込んだ結果の件数」が必ず一致する。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from schemaform.calculated import _apply_aggregate, _collect_numeric_values
from schemaform.fields import flatten_fields, get_nested_value

NUMERIC_TYPES = {"number", "integer", "calculated"}
TEMPORAL_TYPES = {"datetime", "date", "time"}
# カテゴリ（ラベル別件数）として集計できるフィールド型。
CATEGORY_TYPES = {"enum", "boolean", "string", "master", "file"} | TEMPORAL_TYPES
TRUTHY_STRINGS = {"true", "1", "yes", "on"}

EMPTY_LABEL = "(未入力)"
OTHER_LABEL = "その他"
FILE_PRESENT_LABEL = "あり"
FILE_ABSENT_LABEL = "なし"

# カテゴリ集計で個別に表示するラベルの最大数（超過分は「その他」へ集約）。
MAX_CATEGORY_LABELS = 20
# 自由記述はラベルが際限なく増えるため、より少なく絞る。
MAX_TEXT_LABELS = 10
# クロス集計の棒グラフに表示するラベルの最大数。
MAX_CROSSTAB_LABELS = 30
# ヒストグラムのビン数。
HISTOGRAM_BINS = 10


# ---------------------------------------------------------------------------
# 表示用の整形
# ---------------------------------------------------------------------------


def _format_num(value: float) -> str:
    """数値を表示用文字列に整形する（整数値は小数点なし、それ以外は小数2桁）。"""
    rounded = round(float(value), 2)
    if rounded == int(rounded):
        return str(int(rounded))
    return f"{rounded:g}"


def _format_stat(value: float | None) -> str:
    return "—" if value is None else _format_num(value)


def _median(values: list[float]) -> float | None:
    """数値リストの中央値を返す（空なら None）。"""
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _stats_for(values: list[float]) -> dict[str, str]:
    """数値リストから表示用の統計値（件数/合計/平均/中央値/最大/最小）を作る。"""
    return {
        "count": str(len(values)),
        "sum": _format_stat(_apply_aggregate("sum", values)),
        "avg": _format_stat(_apply_aggregate("avg", values)),
        "median": _format_stat(_median(values)),
        "min": _format_stat(_apply_aggregate("min", values)),
        "max": _format_stat(_apply_aggregate("max", values)),
    }


def _metric_values(values: list[float]) -> dict[str, float]:
    """クロス集計の棒グラフで切り替える指標ごとの数値。"""

    def num(value: float | None) -> float:
        return round(value, 2) if value is not None else 0.0

    return {
        "count": float(len(values)),
        "sum": num(_apply_aggregate("sum", values)),
        "avg": num(_apply_aggregate("avg", values)),
        "median": num(_median(values)),
        "max": num(_apply_aggregate("max", values)),
        "min": num(_apply_aggregate("min", values)),
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


# ---------------------------------------------------------------------------
# 値の正規化（集計とドリルダウン判定で共有する）
# ---------------------------------------------------------------------------

MasterLabels = dict[str, dict[str, str]]


def _truthy(value: Any) -> bool:
    return value is True or (
        isinstance(value, str) and value.lower() in TRUTHY_STRINGS
    )


def _temporal_label(field_type: str, raw: str) -> str:
    """日付・日時・時刻を集計しやすい粒度のラベルにする。

    日時は日単位、時刻は時間帯（HH時台）に丸める。生の値のままでは一つ一つが
    別ラベルになってしまい、分布として意味を成さないため。"""
    text = raw.strip()
    if not text:
        return EMPTY_LABEL
    if field_type == "date":
        return text[:10]
    if field_type == "datetime":
        return text[:10]
    # time: "HH:MM" / "HH:MM:SS" → "HH時台"
    head = text[:2]
    return f"{head}時台" if head.isdigit() else text


def category_labels(
    field: dict[str, Any], value: Any, master_labels: MasterLabels | None = None
) -> list[str]:
    """1送信ぶんの値を、カテゴリ集計のラベル列に正規化する。

    配列は要素ごとに1ラベル。未入力は ``EMPTY_LABEL`` を1つ返す（「未回答が何件
    あるか」も分布の一部として数えるため）。"""
    field_type = str(field.get("type", ""))
    flat_key = str(field.get("flat_key", ""))

    if field_type == "file":
        items = value if isinstance(value, list) else [value]
        present = any(item not in (None, "") for item in items)
        return [FILE_PRESENT_LABEL if present else FILE_ABSENT_LABEL]

    items = value if isinstance(value, list) else [value]
    labels: list[str] = []
    for item in items:
        if item is None or item == "":
            labels.append(EMPTY_LABEL)
            continue
        if field_type == "boolean":
            labels.append("true" if _truthy(item) else "false")
        elif field_type == "master":
            lookup = (master_labels or {}).get(flat_key, {})
            labels.append(lookup.get(str(item)) or str(item))
        elif field_type in TEMPORAL_TYPES:
            labels.append(_temporal_label(field_type, str(item)))
        else:
            labels.append(str(item).strip() or EMPTY_LABEL)
    return labels or [EMPTY_LABEL]


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


# ---------------------------------------------------------------------------
# ドリルダウン条件
# ---------------------------------------------------------------------------

DRILL_PARAM = "drill"


def _drill_eq(flat_key: str, label: str) -> dict[str, Any] | None:
    """カテゴリのラベル一致条件。「その他」はひとつの値ではないため作らない。"""
    if label == OTHER_LABEL:
        return None
    return {"k": flat_key, "op": "eq", "v": label}


def _drill_range(
    flat_key: str, low: float, high: float, inclusive: bool
) -> dict[str, Any]:
    return {"k": flat_key, "op": "range", "lo": low, "hi": high, "inc": inclusive}


def parse_drill(raw: str | None) -> list[dict[str, Any]]:
    """URLの ``drill`` パラメータ（JSON配列）を条件リストに変換する。

    壊れた値は黙って捨てる（URLを手で編集された場合にページ全体が落ちないように）。
    """
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []

    conditions: list[dict[str, Any]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        op = str(item.get("op", ""))
        key = str(item.get("k", ""))
        if op == "eq" and key:
            conditions.append({"k": key, "op": "eq", "v": str(item.get("v", ""))})
        elif op == "range" and key:
            try:
                low = float(item.get("lo"))
                high = float(item.get("hi"))
            except (TypeError, ValueError):
                continue
            conditions.append(
                {
                    "k": key,
                    "op": "range",
                    "lo": low,
                    "hi": high,
                    "inc": bool(item.get("inc")),
                }
            )
        elif op == "ts":
            low = to_utc_iso(item.get("lo"))
            high = to_utc_iso(item.get("hi"))
            if low and high:
                # 区間の見出しはブラウザ側がローカル時刻で作る。UTCのまま画面に
                # 出すと閲覧者の時刻とずれるので、その文字列を受け取って表示に使う。
                title = str(item.get("t", ""))[:40]
                conditions.append({"op": "ts", "lo": low, "hi": high, "t": title})
        elif op == "score":
            try:
                conditions.append({"op": "score", "n": int(item.get("n"))})
            except (TypeError, ValueError):
                continue
    return conditions


def encode_drill(conditions: list[dict[str, Any]]) -> str:
    """条件リストをURLに載せる文字列にする（空なら空文字）。"""
    if not conditions:
        return ""
    return json.dumps(conditions, ensure_ascii=False, separators=(",", ":"))


def _field_map(fields: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        field["flat_key"]: field
        for field in flatten_fields(fields, expand_rows_for_group_arrays=True)
    }


def apply_drill(
    submissions: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    conditions: list[dict[str, Any]],
    *,
    master_labels: MasterLabels | None = None,
    score_of: Callable[[dict[str, Any]], int] | None = None,
) -> list[dict[str, Any]]:
    """ドリルダウン条件をすべて満たす送信だけを残す（条件はAND）。

    判定には集計時と同じ :func:`category_labels` を使うので、グラフの棒の高さと
    絞り込み後の件数は一致する。"""
    if not conditions:
        return submissions

    field_map = _field_map(fields)

    def matches(submission: dict[str, Any], condition: dict[str, Any]) -> bool:
        op = condition["op"]
        if op == "ts":
            iso = to_utc_iso(submission.get("created_at"))
            return bool(iso and condition["lo"] <= iso < condition["hi"])
        if op == "score":
            return score_of is not None and score_of(submission) == condition["n"]

        field = field_map.get(condition["k"])
        if field is None:
            return False
        value = get_nested_value(submission.get("data_json", {}), condition["k"])
        if op == "eq":
            return condition["v"] in category_labels(field, value, master_labels)
        if op == "range":
            low, high, inclusive = condition["lo"], condition["hi"], condition["inc"]
            return any(
                low <= number <= high if inclusive else low <= number < high
                for number in _collect_numeric_values(value)
            )
        return False

    return [
        submission
        for submission in submissions
        if all(matches(submission, condition) for condition in conditions)
    ]


def describe_drill(
    conditions: list[dict[str, Any]], fields: list[dict[str, Any]]
) -> list[str]:
    """条件を画面に出す文言にする。"""
    field_map = _field_map(fields)
    texts: list[str] = []
    for condition in conditions:
        op = condition["op"]
        if op == "ts":
            span = condition.get("t") or f"{condition['lo'][:16]}(UTC)"
            texts.append(f"送信日時: {span}")
            continue
        if op == "score":
            texts.append(f"正答数: {condition['n']}")
            continue
        field = field_map.get(condition["k"])
        label = field["flat_label"] if field else condition["k"]
        if op == "eq":
            texts.append(f"{label}: {condition['v']}")
        elif op == "range":
            joiner = "〜" if condition["inc"] else "〜未満"
            texts.append(
                f"{label}: {_format_num(condition['lo'])}{joiner}"
                f"{_format_num(condition['hi'])}"
            )
    return texts


# ---------------------------------------------------------------------------
# ブロックの組み立て
# ---------------------------------------------------------------------------


def chart_spec(
    items: list[dict[str, Any]],
    chart_type: str,
    *,
    label: str = "件数",
    mono: bool = False,
) -> dict[str, Any]:
    """項目リストを、そのまま Chart.js に渡せる形にする。

    ``drills`` は棒・円の1つ1つに対応する絞り込み条件（不可なら null）。"""
    return {
        "type": chart_type,
        "label": label,
        "mono": mono,
        "labels": [item["label"] for item in items],
        "counts": [item["count"] for item in items],
        "drills": [item["drill"] for item in items],
    }


def _category_items(
    counts: dict[str, int],
    order: list[str],
    flat_key: str,
    *,
    max_labels: int,
    sort_by_count: bool,
    keep_other: bool = True,
) -> tuple[list[dict[str, Any]], int]:
    """ラベル→件数を表示用の項目リストにし、超過分を「その他」へ丸める。"""
    rank = {label: index for index, label in enumerate(order)}
    labels = (
        sorted(order, key=lambda label: (-counts[label], rank[label]))
        if sort_by_count
        else list(order)
    )
    other = 0
    if len(labels) > max_labels:
        head = labels[: max_labels - 1]
        other = sum(counts[label] for label in labels[max_labels - 1 :])
        labels = head

    total = sum(counts.values())
    items = [
        {
            "label": label,
            "count": counts[label],
            "ratio": (counts[label] / total) if total else 0.0,
            "drill": _drill_eq(flat_key, label),
        }
        for label in labels
    ]
    if other and keep_other:
        items.append(
            {
                "label": OTHER_LABEL,
                "count": other,
                "ratio": (other / total) if total else 0.0,
                "drill": None,
            }
        )
    return items, other


def build_category_block(
    field: dict[str, Any],
    submissions: list[dict[str, Any]],
    master_labels: MasterLabels | None = None,
) -> dict[str, Any]:
    """選択肢・真偽・自由記述・日付・参照・ファイルの「ラベル別件数」ブロック。"""
    flat_key = field["flat_key"]
    field_type = str(field.get("type", ""))
    counts: dict[str, int] = {}
    order: list[str] = []

    # 選択肢は定義された順序を初期の並びにして、未使用の選択肢も0件で見せる。
    if field_type == "enum":
        for option in field.get("enum") or []:
            label = str(option)
            counts.setdefault(label, 0)
            if label not in order:
                order.append(label)
    elif field_type == "boolean":
        for label in ("true", "false"):
            counts[label] = 0
            order.append(label)

    answered = 0
    for value in _iter_field_values(submissions, flat_key):
        labels = category_labels(field, value, master_labels)
        if any(label != EMPTY_LABEL for label in labels):
            answered += 1
        for label in labels:
            if label not in counts:
                counts[label] = 0
                order.append(label)
            counts[label] += 1

    is_text = field_type == "string"
    # 選択肢・真偽は取りうる値が決まっている（未使用の選択肢も0件で並べる）。
    has_fixed_options = field_type in {"enum", "boolean"}
    items, other = _category_items(
        counts,
        order,
        flat_key,
        max_labels=MAX_TEXT_LABELS if is_text else MAX_CATEGORY_LABELS,
        # 定義順のある選択肢・真偽以外は件数の多い順に並べる。
        sort_by_count=not has_fixed_options,
        # 自由記述は「その他」が巨大な棒になって上位が読めなくなるため、
        # グラフには入れず件数だけ添える。
        keep_other=not is_text,
    )

    # 実際に回答された値の種類数。選択肢の定義数ではないので、0件の選択肢は数えない。
    distinct = sum(
        1 for label in order if counts[label] > 0 and label != EMPTY_LABEL
    )
    # 回答がすべて異なる値なら、頻度のグラフは何も語らない。件数だけ伝える。
    # 取りうる値が決まっている項目は、1人1つずつ違う選択肢を選んだとしてもそれ自体が
    # 分布なので対象にしない。
    all_unique = not has_fixed_options and answered > 5 and distinct >= answered

    return {
        "kind": "category",
        "key": flat_key,
        "label": field["flat_label"],
        "type": field_type,
        "is_array": bool(field.get("is_array")),
        "unique_items": bool(field.get("unique_items")),
        # 選択肢が少ないものは割合が読みやすい円、多いもの・自由記述は棒を既定にする。
        "chart": (
            None
            if all_unique
            else chart_spec(items, "pie" if len(items) <= 6 and not is_text else "bar")
        ),
        "items": items,
        "total": sum(counts.values()),
        "answered": answered,
        "distinct": distinct,
        "other_count": other,
        "note": (
            "すべて異なる値のため、グラフは表示しません"
            if all_unique
            else (f"上位{len(items)}件を表示" if other else "")
        ),
    }


def build_histogram_bins(
    values: list[float], flat_key: str, *, is_integer: bool
) -> list[dict[str, Any]]:
    """数値リストを等幅ビンに集計する。

    値が0/1個・全同値の場合は単一ビンにフォールバックしてゼロ除算を避ける。
    整数かつレンジが狭い場合は値ごとのビンにする。各ビンは自分自身を再現する
    ドリルダウン条件を持つ。"""
    if not values:
        return []

    low = min(values)
    high = max(values)
    if low == high:
        return [
            {
                "label": _format_num(low),
                "count": len(values),
                "drill": _drill_range(flat_key, low, high, True),
            }
        ]

    if (
        is_integer
        and float(low).is_integer()
        and float(high).is_integer()
        and (high - low) <= HISTOGRAM_BINS - 1
    ):
        ilow, ihigh = int(low), int(high)
        counts = [0] * (ihigh - ilow + 1)
        for value in values:
            counts[int(round(value)) - ilow] += 1
        return [
            {
                "label": str(ilow + index),
                "count": count,
                "drill": _drill_range(
                    flat_key, float(ilow + index), float(ilow + index), True
                ),
            }
            for index, count in enumerate(counts)
        ]

    width = (high - low) / HISTOGRAM_BINS
    edges = [low + width * index for index in range(HISTOGRAM_BINS + 1)]
    counts = [0] * HISTOGRAM_BINS
    for value in values:
        index = min(HISTOGRAM_BINS - 1, max(0, int((value - low) / width)))
        counts[index] += 1
    return [
        {
            "label": f"{_format_num(edges[index])}–{_format_num(edges[index + 1])}",
            "count": counts[index],
            # 最終ビンだけ上限を含む半開区間。境界値の二重計上を防ぐ。
            "drill": _drill_range(
                flat_key,
                edges[index],
                edges[index + 1],
                index == HISTOGRAM_BINS - 1,
            ),
        }
        for index in range(HISTOGRAM_BINS)
    ]


def build_numeric_block(
    field: dict[str, Any], submissions: list[dict[str, Any]]
) -> dict[str, Any]:
    """数値・計算フィールドの統計値とヒストグラム。"""
    flat_key = field["flat_key"]
    values: list[float] = []
    for value in _iter_field_values(submissions, flat_key):
        values.extend(_collect_numeric_values(value))
    bins = build_histogram_bins(
        values, flat_key, is_integer=field.get("type") == "integer"
    )
    return {
        "kind": "numeric",
        "key": flat_key,
        "label": field["flat_label"],
        "type": str(field.get("type", "")),
        "stats": _stats_for(values),
        "bins": bins,
        # ヒストグラムは1つの系列を区間に切ったもの。色分けは意味を持たないので単色。
        "chart": chart_spec(bins, "bar", mono=True) if bins else None,
    }


def _parent_prefix(flat_key: str) -> str:
    """ドット区切りの flat_key から親グループのプレフィックスを返す（無ければ空）。"""
    return flat_key.rsplit(".", 1)[0] if "." in flat_key else ""


def _crosstab_group(
    submissions: list[dict[str, Any]],
    num_key: str,
    label_field: dict[str, Any],
    master_labels: MasterLabels | None,
) -> dict[str, Any] | None:
    """数値フィールドを、カテゴリ項目の値ごとに集計する。"""
    label_key = label_field["flat_key"]
    buckets: dict[str, list[float]] = {}
    order: list[str] = []
    rows = 0
    for submission in submissions:
        data = submission.get("data_json", {})
        numbers = _collect_numeric_values(get_nested_value(data, num_key))
        if not numbers:
            continue
        rows += 1
        for label in category_labels(
            label_field, get_nested_value(data, label_key), master_labels
        ):
            if label not in buckets:
                buckets[label] = []
                order.append(label)
            buckets[label].extend(numbers)
    if not buckets:
        return None
    # ほとんどの行が別々のラベルになる項目（氏名や日付など）で束ねても比較にならない。
    # 1ラベルあたり平均2行以上まとまるものだけを「比較できる切り口」とみなす。
    if len(order) > max(2, rows // 2):
        return None

    # 合計の降順に並べ、多すぎる場合は末尾を「その他」へまとめる。
    ordered = sorted(order, key=lambda label: sum(buckets[label]), reverse=True)
    if len(ordered) > MAX_CROSSTAB_LABELS:
        rest = ordered[MAX_CROSSTAB_LABELS - 1 :]
        buckets[OTHER_LABEL] = [
            number for label in rest for number in buckets[label]
        ]
        ordered = ordered[: MAX_CROSSTAB_LABELS - 1] + [OTHER_LABEL]

    # 棒グラフは指標を切り替えて使うため、指標ごとの配列に転置しておく。
    metrics = {name: [] for name in ("count", "sum", "avg", "median", "max", "min")}
    for label in ordered:
        for name, value in _metric_values(buckets[label]).items():
            metrics[name].append(value)

    return {
        "label_key": label_key,
        "label": label_field["flat_label"],
        "labels": ordered,
        "metrics": metrics,
        "stats": [_stats_for(buckets[label]) for label in ordered],
        "drills": [_drill_eq(label_key, label) for label in ordered],
    }


def build_crosstab_block(
    field: dict[str, Any],
    flat_fields: list[dict[str, Any]],
    submissions: list[dict[str, Any]],
    master_labels: MasterLabels | None = None,
) -> dict[str, Any] | None:
    """数値フィールドを、カテゴリ項目ごとに集計（Group By）したブロック。

    集計対象は次のいずれかでペアが成立する組み合わせに限る:

    * ラベルがトップレベル（全行で単一値）。任意の数値を集計できる。
    * ラベルと数値が同じグループ内（同一行でペアになる）。

    別々の配列グループ同士は行展開でデカルト積になり件数が水増しされるため除外する。
    """
    num_key = field["flat_key"]
    num_parent = _parent_prefix(num_key)
    groups: list[dict[str, Any]] = []
    for candidate in flat_fields:
        if candidate.get("type") not in CATEGORY_TYPES:
            continue
        label_parent = _parent_prefix(candidate["flat_key"])
        if label_parent != "" and label_parent != num_parent:
            continue
        group = _crosstab_group(submissions, num_key, candidate, master_labels)
        if group:
            groups.append(group)
    if not groups:
        return None
    # 切り口が少ないものほど比較しやすいので、それを既定の表示にする。
    groups.sort(key=lambda group: len(group["labels"]))
    return {
        "kind": "crosstab",
        "key": num_key,
        "label": field["flat_label"],
        "groups": groups,
    }


def _timeseries_timestamps(submissions: list[dict[str, Any]]) -> list[str]:
    """件数推移グラフ用に、各送信の送信日時（UTC・秒精度のISO文字列）を返す。

    横軸の粒度（秒・分・時・日・週・月・年）は、送信日時の間隔に応じてクライアント
    側で自動選択し、手動でも切り替えられるようにする。"""
    stamps = [
        iso
        for iso in (to_utc_iso(item.get("created_at")) for item in submissions)
        if iso is not None
    ]
    stamps.sort()
    return stamps


# ---------------------------------------------------------------------------
# 用途別テンプレート
# ---------------------------------------------------------------------------

VIEWS: list[dict[str, Any]] = [
    {
        "id": "overview",
        "name": "概要",
        "summary": "件数の推移と主要項目の内訳をひと目で確認します。",
        "blocks": {"kpi", "timeseries", "category"},
        # 概要では、選ばせる項目＝内訳を見て意味のある項目だけに絞る。
        # 自由記述や日付まで並べると、ひと目で分かるという目的から外れるため。
        "category_types": {"enum", "boolean", "master"},
        "table": "preview",
    },
    {
        "id": "fields",
        "name": "項目分析",
        "summary": "項目ごとの分布と数値の統計値を詳しく見ます。",
        "blocks": {"category", "numeric"},
        "category_types": CATEGORY_TYPES,
        "table": "preview",
    },
    {
        "id": "crosstab",
        "name": "クロス集計",
        "summary": "数値を選択肢や日付などの項目別に比較します。",
        "blocks": {"crosstab"},
        "table": "preview",
    },
    {
        "id": "scoring",
        "name": "テスト採点",
        "summary": "選択肢の正解を決めて、設問別の正答率と得点分布を出します。",
        "blocks": {"scoring"},
        "table": "full",
    },
    {
        "id": "raw",
        "name": "元データ",
        "summary": "集計せずに送信内容そのものを確認・ダウンロードします。",
        "blocks": set(),
        "table": "full",
    },
]

DEFAULT_VIEW = "overview"
_VIEW_BY_ID = {view["id"]: view for view in VIEWS}


def resolve_view(view_id: str | None) -> dict[str, Any]:
    """``?view=`` の値からテンプレート定義を引く（不正な値は概要）。"""
    return _VIEW_BY_ID.get(str(view_id or ""), _VIEW_BY_ID[DEFAULT_VIEW])


def available_views(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """フォームの項目構成から、意味のあるテンプレートだけを返す。

    数値項目が無いフォームで「クロス集計」を、選択肢が無いフォームで「テスト採点」を
    出しても空のページになるだけなので、あらかじめ隠す。"""
    flat = flatten_fields(fields, expand_rows_for_group_arrays=True)
    types = {str(field.get("type", "")) for field in flat}
    has_numeric = bool(types & NUMERIC_TYPES)
    has_category = bool(types & CATEGORY_TYPES)
    has_enum = "enum" in types

    result = []
    for view in VIEWS:
        if view["id"] == "crosstab" and not (has_numeric and has_category):
            continue
        if view["id"] == "scoring" and not has_enum:
            continue
        if view["id"] == "fields" and not (has_numeric or has_category):
            continue
        result.append(view)
    return result


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------


def _kpi(
    submissions: list[dict[str, Any]], distinct: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """ページ冒頭に出す要約値。"""
    stamps = _timeseries_timestamps(distinct)
    kpi: list[dict[str, Any]] = [
        {"label": "送信数", "value": str(len(distinct)), "note": "", "iso": ""}
    ]
    if len(submissions) != len(distinct):
        # 配列グループを展開しているため、集計上の行数は送信数と一致しない。
        kpi.append(
            {
                "label": "集計対象の行数",
                "value": str(len(submissions)),
                "note": "配列グループを展開した行数",
                "iso": "",
            }
        )
    if stamps:
        # 日時は絶対時刻を渡し、表示は閲覧者のローカル時刻に任せる（表と同じ基準）。
        kpi.append(
            {"label": "最初の送信", "value": stamps[0][:16], "note": "", "iso": stamps[0]}
        )
        kpi.append(
            {"label": "最新の送信", "value": stamps[-1][:16], "note": "", "iso": stamps[-1]}
        )
    return kpi


def aggregate_submissions(
    fields: list[dict[str, Any]],
    submissions: list[dict[str, Any]],
    *,
    view: dict[str, Any] | None = None,
    master_labels: MasterLabels | None = None,
) -> dict[str, Any]:
    """フィールド定義と送信一覧から、テンプレートが必要とするブロックだけを作る。

    submissions は配列グループ展開済み（送信一覧と同じ）を想定する。件数・時系列は
    送信 id 単位に一意化して実際の送信数を反映する。
    """
    view = view or resolve_view(DEFAULT_VIEW)
    wanted = view["blocks"]
    flat_fields = flatten_fields(fields, expand_rows_for_group_arrays=True)
    distinct = _distinct_submissions(submissions)

    blocks: list[dict[str, Any]] = []
    if "category" in wanted:
        category_types = view.get("category_types") or CATEGORY_TYPES
        blocks.extend(
            build_category_block(field, submissions, master_labels)
            for field in flat_fields
            if field.get("type") in category_types
        )
    if "numeric" in wanted:
        blocks.extend(
            build_numeric_block(field, submissions)
            for field in flat_fields
            if field.get("type") in NUMERIC_TYPES
        )
    if "crosstab" in wanted:
        for field in flat_fields:
            if field.get("type") not in NUMERIC_TYPES:
                continue
            block = build_crosstab_block(
                field, flat_fields, submissions, master_labels
            )
            if block:
                blocks.append(block)

    return {
        "blocks": blocks,
        "kpi": _kpi(submissions, distinct) if "kpi" in wanted else [],
        "timeseries_timestamps": (
            _timeseries_timestamps(distinct) if "timeseries" in wanted else []
        ),
        "total_submissions": len(distinct),
    }


def master_label_map(
    master_lookup_by_field: dict[str, dict[str, dict[str, Any]]],
) -> MasterLabels:
    """送信一覧が持つフォーム参照の解決表を、ID→表示ラベルの形に変換する。"""
    return {
        flat_key: {
            record_id: str(record.get("label", "") or record_id)
            for record_id, record in lookup.items()
        }
        for flat_key, lookup in master_lookup_by_field.items()
    }


def collect_enum_fields(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """テスト採点で正解を設定できるフィールド（選択肢）を返す。"""
    return [
        field
        for field in flatten_fields(fields, expand_rows_for_group_arrays=True)
        if field.get("type") == "enum"
    ]
