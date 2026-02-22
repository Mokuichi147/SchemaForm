from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlsplit

import httpx

from schemaform.utils import to_iso

logger = logging.getLogger(__name__)


def is_valid_webhook_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlsplit(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


async def send_webhook(
    url: str,
    event: str,
    form: dict[str, Any],
    submission: dict[str, Any] | None = None,
) -> bool:
    if not is_valid_webhook_url(url):
        return False

    payload: dict[str, Any] = {
        "event": event,
        "form_id": form.get("id"),
        "form_name": form.get("name"),
        "form_public_id": form.get("public_id"),
    }

    if submission:
        payload["submission_id"] = submission.get("id")
        payload["data"] = submission.get("data_json", {})
        if submission.get("created_at"):
            payload["created_at"] = to_iso(submission["created_at"])

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
        logger.info("Webhook sent successfully: %s -> %s", event, url)
        return True
    except Exception:
        logger.exception("Webhook failed: %s -> %s", event, url)
        return False
