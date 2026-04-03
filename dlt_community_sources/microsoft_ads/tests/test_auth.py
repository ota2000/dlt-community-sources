"""Tests for Microsoft Ads auth helpers."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_community_sources.microsoft_ads.auth import (
    MSFT_TOKEN_URL,
    refresh_access_token,
)


def _mock_response(json_data):
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.raise_for_status.return_value = None
    return mock


class TestRefreshAccessToken:
    @patch("dlt_community_sources.microsoft_ads.auth.requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = _mock_response(
            {
                "access_token": "new_at",
                "refresh_token": "new_rt",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )
        result = refresh_access_token("client_id", "client_secret", "old_rt")
        assert result["access_token"] == "new_at"
        assert result["refresh_token"] == "new_rt"
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["data"]["grant_type"] == "refresh_token"
        assert call_kwargs[1]["data"]["client_id"] == "client_id"
        assert call_kwargs[1]["data"]["refresh_token"] == "old_rt"

    @patch("dlt_community_sources.microsoft_ads.auth.requests.post")
    def test_error_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("HTTP 400")
        mock_post.return_value = mock_resp
        with pytest.raises(Exception, match="HTTP 400"):
            refresh_access_token("client_id", "client_secret", "bad_rt")


def test_token_url():
    assert "oauth2/v2.0/token" in MSFT_TOKEN_URL
