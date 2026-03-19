from __future__ import annotations

import ast
import re
from typing import Any

FIELD_REF_PATTERN = re.compile(r"\{([A-Za-z][A-Za-z0-9_.]*)\}")

AGGREGATE_FUNCTIONS = {"sum", "avg", "count", "max", "min"}


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


AGGREGATE_CALL_PATTERN = re.compile(
    r"(sum|avg|count|max|min)\(\{([A-Za-z][A-Za-z0-9_.]*)\}\)"
)


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


def validate_formula(formula: str) -> list[str]:
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
