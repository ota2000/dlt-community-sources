"""HTTP client for NextDNS API."""

import logging
import time
from typing import Any, Generator, Optional

import requests

BASE_URL = "https://api.nextdns.io"

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0


class NextDNSClient:
    """Client for NextDNS API with API key auth and rate limit handling."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self._session = requests.Session()
        self._session.headers.update(
            {"X-Api-Key": api_key, "Accept": "application/json"}
        )

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        for attempt in range(MAX_RETRIES):
            response = self._session.request(method, url, **kwargs)

            if response.status_code == 429:
                backoff = INITIAL_BACKOFF * (2**attempt)
                logger.warning(
                    "Rate limited (429). Retrying in %.1fs (attempt %d/%d)",
                    backoff,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(backoff)
                continue

            response.raise_for_status()
            return response

        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def get(self, path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        url = f"{BASE_URL}/{path}"
        return self._request("GET", url, params=params).json()

    def get_paginated(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch all pages using cursor-based pagination."""
        if params is None:
            params = {}

        url = f"{BASE_URL}/{path}"
        while True:
            try:
                response = self._request("GET", url, params=params)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code in (403, 404):
                    logger.warning(
                        "Request failed (%d) for %s. Skipping.",
                        e.response.status_code,
                        path,
                    )
                    return
                raise
            data = response.json()
            yield from data.get("data", [])
            cursor = data.get("meta", {}).get("pagination", {}).get("cursor")
            if not cursor:
                break
            params["cursor"] = cursor
