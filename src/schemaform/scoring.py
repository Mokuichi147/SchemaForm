"""選択肢フィールドの「正解」を決めて採点する（集計ページのテスト採点テンプレート）。

正解の指定はURLの ``correct`` パラメータ（JSON）で受け渡す。画面表示・得点分布・
ダウンロードのすべてがこの1か所の判定を通るため、表示とファイルの内容がずれない。
"""

from __future__ import annotations

import json
from typing import Any

from schemaform.aggregate import _distinct_submissions, chart_spec
from schemaform.fields import get_nested_value

CORRECT_PARAM = "correct"


def parse_correct_map(raw: str | None) -> dict[str, dict[str, Any]]:
    """正解指定(JSON)をパースする。

    形式: {flat_key: {"mode": "single", "value": str}}
          {flat_key: {"mode": "array", "list": [str], "ordered": bool}}
    """
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError, TypeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for key, info in parsed.items():
        if not isinstance(info, dict):
            continue
        if info.get("mode") == "array":
            values = info.get("list")
            if isinstance(values, list) and values:
                result[str(key)] = {
                    "mode": "array",
                    "list": [str(v) for v in values],
                    "ordered": bool(info.get("ordered")),
                }
        else:
            value = info.get("value")
            if value not in (None, ""):
                result[str(key)] = {"mode": "single", "value": str(value)}
    return result


def encode_correct_map(correct_map: dict[str, dict[str, Any]]) -> str:
    """正解指定をURLに載せる文字列にする（空なら空文字）。"""
    if not correct_map:
        return ""
    return json.dumps(correct_map, ensure_ascii=False, separators=(",", ":"))


def is_answered(value: Any) -> bool:
    if isinstance(value, list):
        return len(value) > 0
    return value not in (None, "")


def row_is_correct(data: dict[str, Any], flat_key: str, info: dict[str, Any]) -> bool:
    """1送信の1設問が正解かどうか。

    複数選択（配列）は重複・件数も含めた過不足のない完全一致。``ordered`` を指定した
    場合は選んだ順序も一致していることを求める。"""
    value = get_nested_value(data, flat_key)
    if value is None:
        return False
    if info["mode"] == "array":
        actual = [str(v) for v in (value if isinstance(value, list) else [value])]
        expected = info["list"]
        if len(actual) != len(expected):
            return False
        if info.get("ordered"):
            return actual == expected
        return sorted(actual) == sorted(expected)
    if isinstance(value, list):
        return False
    return str(value) == info["value"]


def score_of(data: dict[str, Any], correct_map: dict[str, dict[str, Any]]) -> int:
    """1送信の正答数。"""
    return sum(
        1 for key, info in correct_map.items() if row_is_correct(data, key, info)
    )


def correctness_cells(
    data: dict[str, Any], correct_map: dict[str, dict[str, Any]]
) -> tuple[str, str]:
    """1送信あたりの (正答数, 正答率) セル文字列を返す。正答数は個数のみ、
    正答率は%なしの数値。画面表示と同じ表記。"""
    total = len(correct_map)
    if total == 0:
        return "", ""
    correct = score_of(data, correct_map)
    return str(correct), str(round(correct / total * 100))


def build_scoring(
    enum_fields: list[dict[str, Any]],
    submissions: list[dict[str, Any]],
    correct_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """テスト採点テンプレートの表示内容を組み立てる。

    採点は送信単位で行うため、配列グループ展開で増えた行は id で一意化してから数える。
    """
    distinct = _distinct_submissions(submissions)
    # 正解を設定できる設問の一覧（設定済みかどうかに関わらず入力UIを出す）。
    questions: list[dict[str, Any]] = []
    for field in enum_fields:
        flat_key = field["flat_key"]
        info = correct_map.get(flat_key)
        answered = 0
        correct = 0
        for submission in distinct:
            data = submission.get("data_json", {})
            if not is_answered(get_nested_value(data, flat_key)):
                continue
            answered += 1
            if info and row_is_correct(data, flat_key, info):
                correct += 1
        questions.append(
            {
                "key": flat_key,
                "label": field["flat_label"],
                "is_array": bool(field.get("is_array")),
                "unique_items": bool(field.get("unique_items")),
                "options": [str(option) for option in (field.get("enum") or [])],
                "correct_info": info,
                "answered": answered,
                "correct": correct,
                "rate": (correct / answered * 100) if (info and answered) else None,
            }
        )

    total_questions = len(correct_map)
    if not total_questions:
        return {
            "questions": questions,
            "configured": 0,
            "graded": 0,
            "average_rate": None,
            "average_score": None,
            "chart": None,
        }

    counts = [0] * (total_questions + 1)
    total_score = 0
    for submission in distinct:
        score = score_of(submission.get("data_json", {}), correct_map)
        counts[score] += 1
        total_score += score
    graded = len(distinct)

    distribution = [
        {"label": str(score), "count": count, "drill": {"op": "score", "n": score}}
        for score, count in enumerate(counts)
    ]
    return {
        "questions": questions,
        "configured": total_questions,
        "graded": graded,
        "average_score": (total_score / graded) if graded else None,
        "average_rate": (
            (total_score / (graded * total_questions) * 100) if graded else None
        ),
        "chart": chart_spec(distribution, "bar", label="人数", mono=True),
    }
