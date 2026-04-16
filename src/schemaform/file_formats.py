from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

_EXT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._+-]*$")

FILE_FORMAT_ALL = ""
FILE_FORMAT_IMAGE = "image"
FILE_FORMAT_VIDEO = "video"
FILE_FORMAT_AUDIO = "audio"
FILE_FORMAT_DOCUMENT = "document"

VALID_FILE_FORMATS = {
    FILE_FORMAT_ALL,
    FILE_FORMAT_IMAGE,
    FILE_FORMAT_VIDEO,
    FILE_FORMAT_AUDIO,
    FILE_FORMAT_DOCUMENT,
}

FILE_ACCEPT_BY_FORMAT = {
    FILE_FORMAT_IMAGE: "image/*",
    FILE_FORMAT_VIDEO: "video/*",
    FILE_FORMAT_AUDIO: "audio/*",
    FILE_FORMAT_DOCUMENT: (
        ".pdf,.doc,.docx,.xls,.xlsx,.ppt,.pptx,.txt,.rtf,.csv,.tsv,.odt,.ods,.odp"
    ),
}

IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".bmp",
    ".svg",
    ".heic",
    ".heif",
    ".tif",
    ".tiff",
    ".avif",
    ".ico",
}
VIDEO_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".avi",
    ".mkv",
    ".webm",
    ".m4v",
    ".mpeg",
    ".mpg",
    ".wmv",
    ".flv",
}
AUDIO_EXTENSIONS = {
    ".mp3",
    ".wav",
    ".aac",
    ".m4a",
    ".ogg",
    ".flac",
    ".wma",
}
DOCUMENT_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".txt",
    ".rtf",
    ".csv",
    ".tsv",
    ".odt",
    ".ods",
    ".odp",
}

DOCUMENT_MIME_TYPES = {
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/rtf",
    "application/vnd.oasis.opendocument.text",
    "application/vnd.oasis.opendocument.spreadsheet",
    "application/vnd.oasis.opendocument.presentation",
    "text/plain",
    "text/csv",
    "text/tab-separated-values",
}


def _iter_extension_tokens(values: object) -> Iterable[str]:
    if values is None:
        return []
    if isinstance(values, str):
        return values.split(",")
    if isinstance(values, list):
        return [str(item) for item in values]
    return []


def normalize_extension(value: object) -> str:
    token = str(value or "").strip().lower()
    if not token:
        return ""
    token = token.lstrip(".")
    if not token or not _EXT_PATTERN.fullmatch(token):
        return ""
    return f".{token}"


def normalize_file_format(value: object) -> str:
    key = str(value or "").strip().lower()
    return key if key in VALID_FILE_FORMATS else ""


def parse_allowed_extensions(values: object) -> tuple[list[str], list[str]]:
    normalized: list[str] = []
    invalid: list[str] = []
    seen: set[str] = set()
    for raw in _iter_extension_tokens(values):
        raw_text = str(raw or "").strip()
        if not raw_text:
            continue
        ext = normalize_extension(raw_text)
        if not ext:
            invalid.append(raw_text)
            continue
        if ext in seen:
            continue
        seen.add(ext)
        normalized.append(ext)
    return normalized, invalid


def normalize_allowed_extensions(values: object) -> list[str]:
    normalized, _ = parse_allowed_extensions(values)
    return normalized


def file_accept_for_extensions(values: object) -> str:
    return ",".join(normalize_allowed_extensions(values))


def file_accept_for_format(value: object) -> str:
    file_format = normalize_file_format(value)
    return FILE_ACCEPT_BY_FORMAT.get(file_format, "")


def file_accept_for_constraints(file_format: object, allowed_extensions: object) -> str:
    explicit_accept = file_accept_for_extensions(allowed_extensions)
    if explicit_accept:
        return explicit_accept
    return file_accept_for_format(file_format)


def upload_matches_allowed_extensions(filename: object, values: object) -> bool:
    allowed_extensions = normalize_allowed_extensions(values)
    if not allowed_extensions:
        return True
    name = Path(str(filename or "")).name.lower()
    if not name:
        return False
    return any(name.endswith(ext) for ext in allowed_extensions)


def upload_matches_file_format(content_type: object, filename: object, value: object) -> bool:
    file_format = normalize_file_format(value)
    if not file_format:
        return True

    media_type = str(content_type or "").strip().lower()
    extension = normalize_extension(Path(str(filename or "")).suffix)

    if file_format == FILE_FORMAT_IMAGE:
        return media_type.startswith("image/") or extension in IMAGE_EXTENSIONS
    if file_format == FILE_FORMAT_VIDEO:
        return media_type.startswith("video/") or extension in VIDEO_EXTENSIONS
    if file_format == FILE_FORMAT_AUDIO:
        return media_type.startswith("audio/") or extension in AUDIO_EXTENSIONS
    if file_format == FILE_FORMAT_DOCUMENT:
        return (
            media_type in DOCUMENT_MIME_TYPES
            or media_type.startswith("text/")
            or extension in DOCUMENT_EXTENSIONS
        )
    return True


def upload_matches_file_constraints(
    content_type: object,
    filename: object,
    file_format: object,
    allowed_extensions: object,
) -> bool:
    if normalize_allowed_extensions(allowed_extensions):
        return upload_matches_allowed_extensions(filename, allowed_extensions)
    return upload_matches_file_format(content_type, filename, file_format)


def media_kind_for_file(content_type: object, filename: object) -> str:
    """ブラウザ上で表示/再生できるメディア種別を返す。

    該当しない場合は空文字を返す。
    """
    media_type = str(content_type or "").strip().lower()
    if media_type.startswith("image/"):
        return FILE_FORMAT_IMAGE
    if media_type.startswith("video/"):
        return FILE_FORMAT_VIDEO
    if media_type.startswith("audio/"):
        return FILE_FORMAT_AUDIO

    extension = normalize_extension(Path(str(filename or "")).suffix)
    if not extension:
        return ""
    if extension in IMAGE_EXTENSIONS:
        return FILE_FORMAT_IMAGE
    if extension in VIDEO_EXTENSIONS:
        return FILE_FORMAT_VIDEO
    if extension in AUDIO_EXTENSIONS:
        return FILE_FORMAT_AUDIO
    return ""
