from __future__ import annotations

import ast
import re
from typing import Any

# 内部用: キーベースの参照パターン (評価時に使用)
FIELD_REF_PATTERN = re.compile(r"\{([A-Za-z][A-Za-z0-9_.]*)\}")

# ユーザー入力用: ラベル等あらゆる文字を許容する参照パターン
DISPLAY_REF_PATTERN = re.compile(r"\{([^}]+)\}")

AGGREGATE_FUNCTIONS = {"sum", "avg", "count", "max", "min"}

# 内部用: キーベースの集約パターン
AGGREGATE_CALL_PATTERN = re.compile(
    r"(sum|avg|count|max|min)\(\{([A-Za-z][A-Za-z0-9_.]*)\}\)"
)

# ユーザー入力用: ラベルベースの集約パターン
DISPLAY_AGGREGATE_PATTERN = re.compile(
    r"(sum|avg|count|max|min)\(\{([^}]+)\}\)"
)


def extract_field_refs(formula: str) -> list[str]:
    return FIELD_REF_PATTERN.findall(formula)


def _resolve_value(data: dict[str, Any], dotted_key: str) -> Any:
    parts = dotted_key.split(".")
    current: Any = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _collect_numeric_values(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, list):
        result: list[float] = []
        for item in value:
            if isinstance(item, (int, float)) and not isinstance(item, bool):
                result.append(float(item))
            elif isinstance(item, dict):
                for v in item.values():
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        result.append(float(v))
        return result
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return [float(value)]
    return []


def _apply_aggregate(func_name: str, values: list[float]) -> float | None:
    if func_name == "count":
        return float(len(values))
    if not values:
        return None
    if func_name == "sum":
        return sum(values)
    if func_name == "avg":
        return sum(values) / len(values)
    if func_name == "max":
        return max(values)
    if func_name == "min":
        return min(values)
    return None


def _substitute_aggregates(formula: str, data: dict[str, Any]) -> str:
    def _replace_aggregate(match: re.Match) -> str:
        func_name = match.group(1)
        field_ref = match.group(2)
        value = _resolve_value(data, field_ref)
        numeric_values = _collect_numeric_values(value)
        result = _apply_aggregate(func_name, numeric_values)
        if result is None:
            return "0"
        return repr(result)

    return AGGREGATE_CALL_PATTERN.sub(_replace_aggregate, formula)


def _substitute_field_refs(formula: str, data: dict[str, Any]) -> str:
    def _replace_ref(match: re.Match) -> str:
        field_ref = match.group(1)
        value = _resolve_value(data, field_ref)
        if value is None:
            return "0"
        if isinstance(value, bool):
            return "0"
        if isinstance(value, (int, float)):
            return repr(value)
        return "0"

    return FIELD_REF_PATTERN.sub(_replace_ref, formula)


_ALLOWED_NODE_TYPES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Constant,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.USub,
    ast.UAdd,
)


def _safe_eval(expr: str) -> float | None:
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError:
        return None

    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODE_TYPES):
            return None

    try:
        result = eval(compile(tree, "<formula>", "eval"))  # noqa: S307
    except (ZeroDivisionError, TypeError, ValueError, OverflowError):
        return None

    if isinstance(result, (int, float)) and not isinstance(result, bool):
        return float(result)
    return None


def evaluate_formula(formula: str, data: dict[str, Any]) -> float | None:
    if not formula or not formula.strip():
        return None
    expr = _substitute_aggregates(formula, data)
    expr = _substitute_field_refs(expr, data)
    return _safe_eval(expr)


def validate_formula_syntax(formula: str) -> list[str]:
    """キーベースに変換済みの計算式の構文を検証する。"""
    errors: list[str] = []
    if not formula or not formula.strip():
        errors.append("計算式が空です")
        return errors

    test_expr = AGGREGATE_CALL_PATTERN.sub("0", formula)
    test_expr = FIELD_REF_PATTERN.sub("0", test_expr)

    try:
        tree = ast.parse(test_expr, mode="eval")
    except SyntaxError:
        errors.append("計算式の構文が不正です")
        return errors

    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODE_TYPES):
            errors.append("計算式に許可されていない要素が含まれています")
            return errors

    return errors


def formula_labels_to_keys(
    formula: str,
    sibling_fields: list[dict[str, Any]],
) -> tuple[str, list[str]]:
    """ユーザーが入力したラベルベースの計算式をキーベースに変換する。

    例: ``{単価} * {数量}`` → ``{price} * {qty}``
    """
    if not formula or not formula.strip():
        return formula, []

    label_to_key: dict[str, str] = {}
    key_set: set[str] = set()
    for field in sibling_fields:
        label = field.get("label", "").strip()
        key = field.get("key", "")
        if label:
            label_to_key[label] = key
        if key:
            key_set.add(key)

    errors: list[str] = []

    def _replace_aggregate(match: re.Match) -> str:
        func_name = match.group(1)
        ref = match.group(2).strip()
        if ref in key_set:
            return f"{func_name}({{{ref}}})"
        if ref in label_to_key:
            return f"{func_name}({{{label_to_key[ref]}}})"
        errors.append(f"計算式のフィールド「{ref}」が見つかりません")
        return match.group(0)

    result = DISPLAY_AGGREGATE_PATTERN.sub(_replace_aggregate, formula)

    def _replace_ref(match: re.Match) -> str:
        ref = match.group(1).strip()
        if ref in key_set:
            return f"{{{ref}}}"
        if ref in label_to_key:
            return f"{{{label_to_key[ref]}}}"
        errors.append(f"計算式のフィールド「{ref}」が見つかりません")
        return match.group(0)

    result = DISPLAY_REF_PATTERN.sub(_replace_ref, result)
    return result, errors


def formula_keys_to_labels(
    formula: str,
    sibling_fields: list[dict[str, Any]],
) -> str:
    """キーベースの計算式をラベルベースに変換する（表示用）。

    例: ``{price} * {qty}`` → ``{単価} * {数量}``
    """
    if not formula or not formula.strip():
        return formula

    key_to_label: dict[str, str] = {}
    for field in sibling_fields:
        key = field.get("key", "")
        label = field.get("label", "").strip()
        if key and label:
            key_to_label[key] = label

    def _replace_aggregate(match: re.Match) -> str:
        func_name = match.group(1)
        ref = match.group(2)
        label = key_to_label.get(ref, ref)
        return f"{func_name}({{{label}}})"

    result = AGGREGATE_CALL_PATTERN.sub(_replace_aggregate, formula)

    def _replace_ref(match: re.Match) -> str:
        ref = match.group(1)
        label = key_to_label.get(ref, ref)
        return f"{{{label}}}"

    result = FIELD_REF_PATTERN.sub(_replace_ref, result)
    return result


def check_all_refs_required(
    formula: str,
    field_map: dict[str, dict[str, Any]],
) -> bool:
    refs = extract_field_refs(formula)
    if not refs:
        return False
    for ref in refs:
        root_key = ref.split(".")[0]
        field = field_map.get(root_key)
        if not field or not field.get("required"):
            return False
    return True
