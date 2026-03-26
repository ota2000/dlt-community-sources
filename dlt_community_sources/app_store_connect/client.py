"""HTTP client for App Store Connect API."""

import csv
import gzip
import io
import logging
import time
from typing import Any, Generator, Optional

import requests

from .auth import generate_token

BASE_URL = "https://api.appstoreconnect.apple.com/v1"

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds


class AppStoreConnectClient:
    """Client for App Store Connect API with automatic JWT refresh and rate limit handling."""

    def __init__(
        self,
        key_id: str,
        issuer_id: str,
        private_key: str,
    ) -> None:
        self.key_id = key_id
        self.issuer_id = issuer_id
        self.private_key = private_key
        self._session = requests.Session()
        self._refresh_token()

    def _refresh_token(self) -> None:
        token = generate_token(self.key_id, self.issuer_id, self.private_key)
        self._session.headers.update({"Authorization": f"Bearer {token}"})

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        token_refreshed = False
        for attempt in range(MAX_RETRIES):
            response = self._session.request(method, url, **kwargs)

            if response.status_code == 401 and not token_refreshed:
                self._refresh_token()
                token_refreshed = True
                continue

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

        # Final attempt after all retries exhausted
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def get(self, path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        url = f"{BASE_URL}/{path}" if not path.startswith("http") else path
        return self._request("GET", url, params=params).json()

    def get_paginated(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch all pages from a paginated endpoint.

        Yields nothing (instead of crashing) if the endpoint returns 403.
        """
        url = f"{BASE_URL}/{path}"
        while url:
            try:
                response = self._request("GET", url, params=params)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    logger.warning(
                        "Access denied (403) for %s. "
                        "Check your API key permissions. Skipping.",
                        path,
                    )
                    return
                raise
            data = response.json()
            yield from data.get("data", [])
            url = data.get("links", {}).get("next")
            params = None  # params are included in the next URL

    def download(self, url: str) -> bytes:
        return self._request("GET", url).content

    def download_tsv(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, str]]:
        """Download a TSV report and parse it into a list of dicts.

        Returns empty list if the report is not available (404) or not authorized (403).
        """
        try:
            response = self._request("GET", url, params=params)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (403, 404):
                logger.warning(
                    "Report not available (%d) for %s. Skipping.",
                    e.response.status_code,
                    url,
                )
                return []
            raise
        content = response.content
        # Reports may be gzip-compressed
        try:
            content = gzip.decompress(content)
        except gzip.BadGzipFile:
            pass
        text = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        return list(reader)

    def download_gzip_tsv(self, url: str) -> list[dict[str, str]]:
        """Download a gzip-compressed TSV and parse it.

        Returns empty list if download fails.
        """
        try:
            content = self.download(url)
            text = gzip.decompress(content).decode("utf-8")
        except (
            requests.exceptions.HTTPError,
            gzip.BadGzipFile,
            UnicodeDecodeError,
        ) as e:
            logger.warning("Failed to download/decompress TSV from %s: %s", url, e)
            return []
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        return list(reader)
