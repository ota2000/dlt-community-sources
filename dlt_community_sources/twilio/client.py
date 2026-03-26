"""HTTP client for Twilio API."""

import logging
import time
from typing import Any, Generator, Optional

import requests

BASE_URL = "https://api.twilio.com/2010-04-01"

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0


class TwilioClient:
    """Client for Twilio API with Basic Auth and rate limit handling."""

    def __init__(
        self,
        account_sid: str,
        auth_token: Optional[str] = None,
        api_key_sid: Optional[str] = None,
        api_key_secret: Optional[str] = None,
    ) -> None:
        self.account_sid = account_sid
        self._session = requests.Session()
        if api_key_sid and api_key_secret:
            self._session.auth = (api_key_sid, api_key_secret)
        elif auth_token:
            self._session.auth = (account_sid, auth_token)
        else:
            raise ValueError(
                "Provide either auth_token or both api_key_sid and api_key_secret"
            )
        self._session.headers.update({"Accept": "application/json"})

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        for attempt in range(MAX_RETRIES):
            response = self._session.request(method, url, **kwargs)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                backoff = (
                    float(retry_after)
                    if retry_after
                    else INITIAL_BACKOFF * (2**attempt)
                )
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
        url = f"{BASE_URL}/Accounts/{self.account_sid}/{path}.json"
        return self._request("GET", url, params=params).json()

    def get_paginated(
        self,
        path: str,
        resource_key: str,
        params: Optional[dict[str, Any]] = None,
        page_size: int = 100,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch all pages from a Twilio list endpoint.

        Twilio uses next_page_uri for pagination.
        """
        if params is None:
            params = {}
        params["PageSize"] = page_size

        url = f"{BASE_URL}/Accounts/{self.account_sid}/{path}.json"
        while url:
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
            yield from data.get(resource_key, [])
            next_uri = data.get("next_page_uri")
            url = f"https://api.twilio.com{next_uri}" if next_uri else None
            params = None  # params are in the next_page_uri
