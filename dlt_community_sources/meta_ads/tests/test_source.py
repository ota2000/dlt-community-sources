"""Tests for Meta Ads source configuration and helpers."""

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from dlt.sources.helpers import requests as req

from dlt_community_sources.meta_ads.source import (
    DEFAULT_BASE_URL,
    DEFAULT_FIELDS,
    DEFAULT_INSIGHT_FIELDS,
    INSIGHT_FLOAT_FIELDS,
    INSIGHT_INT_FIELDS,
    INSIGHTS_PRIMARY_KEYS,
    POLL_INTERVAL_SECONDS,
    _convert_insight_types,
    _get_paginated,
    _make_client,
    _poll_report,
    _rest_api_config,
    discover_accounts,
)


class TestRestApiConfig:
    """Test _rest_api_config() structure."""

    def setup_method(self):
        self.config = _rest_api_config(
            access_token="test_token",
            account_id="123456",
            base_url="https://graph.facebook.com/v25.0",
        )

    def test_client_base_url(self):
        assert self.config["client"]["base_url"] == "https://graph.facebook.com/v25.0/"

    def test_client_auth(self):
        auth = self.config["client"]["auth"]
        assert auth["type"] == "bearer"
        assert auth["token"] == "test_token"

    def test_paginator(self):
        paginator = self.config["client"]["paginator"]
        assert paginator["type"] == "json_link"
        assert paginator["next_url_path"] == "paging.next"

    def test_resource_defaults(self):
        defaults = self.config["resource_defaults"]
        assert defaults["primary_key"] == "id"
        assert defaults["write_disposition"] == "merge"

    def test_resource_names(self):
        names = [r["name"] for r in self.config["resources"]]
        assert names == [
            "ad_accounts",
            "ad_labels",
            "campaigns",
            "ad_sets",
            "ads",
            "ad_creatives",
            "custom_audiences",
            "custom_conversions",
            "ad_images",
            "ad_videos",
            "activities",
            "saved_audiences",
        ]

    def test_account_id_prefix(self):
        config = _rest_api_config("token", "123456", "https://example.com")
        paths = [r["endpoint"]["path"] for r in config["resources"]]
        assert all(p.startswith("act_123456") for p in paths)

    def test_account_id_already_prefixed(self):
        config = _rest_api_config("token", "act_123456", "https://example.com")
        paths = [r["endpoint"]["path"] for r in config["resources"]]
        assert all(p.startswith("act_123456") for p in paths)
        assert not any("act_act_" in p for p in paths)

    def test_campaign_fields(self):
        campaigns = [r for r in self.config["resources"] if r["name"] == "campaigns"][0]
        fields_param = campaigns["endpoint"]["params"]["fields"]
        for field in DEFAULT_FIELDS["campaigns"]:
            assert field in fields_param

    def test_response_actions(self):
        actions = self.config["resource_defaults"]["endpoint"]["response_actions"]
        status_codes = [a["status_code"] for a in actions]
        assert 400 in status_codes
        assert 403 in status_codes
        assert 404 in status_codes

    def test_ad_images_primary_key(self):
        ad_images = [r for r in self.config["resources"] if r["name"] == "ad_images"][0]
        assert ad_images["primary_key"] == "hash"

    def test_activities_write_disposition(self):
        activities = [r for r in self.config["resources"] if r["name"] == "activities"][
            0
        ]
        assert activities["write_disposition"] == "append"

    def test_activities_no_primary_key(self):
        activities = [r for r in self.config["resources"] if r["name"] == "activities"][
            0
        ]
        assert activities["primary_key"] == ""

    def test_ad_accounts_data_selector(self):
        ad_accounts = [
            r for r in self.config["resources"] if r["name"] == "ad_accounts"
        ][0]
        assert ad_accounts["endpoint"]["data_selector"] == "$"

    def test_ad_accounts_endpoint_path(self):
        ad_accounts = [
            r for r in self.config["resources"] if r["name"] == "ad_accounts"
        ][0]
        assert ad_accounts["endpoint"]["path"] == "act_123456"

    def test_ad_accounts_fields(self):
        ad_accounts = [
            r for r in self.config["resources"] if r["name"] == "ad_accounts"
        ][0]
        fields_param = ad_accounts["endpoint"]["params"]["fields"]
        for field in DEFAULT_FIELDS["ad_accounts"]:
            assert field in fields_param

    def test_ad_labels_endpoint_path(self):
        ad_labels = [r for r in self.config["resources"] if r["name"] == "ad_labels"][0]
        assert ad_labels["endpoint"]["path"] == "act_123456/adlabels"

    def test_ad_labels_fields(self):
        ad_labels = [r for r in self.config["resources"] if r["name"] == "ad_labels"][0]
        fields_param = ad_labels["endpoint"]["params"]["fields"]
        for field in DEFAULT_FIELDS["ad_labels"]:
            assert field in fields_param

    def test_custom_fields_override(self):
        config = _rest_api_config(
            "token",
            "123",
            "https://example.com",
            custom_fields={"campaigns": ["id", "name"]},
        )
        campaigns = [r for r in config["resources"] if r["name"] == "campaigns"][0]
        assert campaigns["endpoint"]["params"]["fields"] == "id,name"


class TestDefaultFields:
    """Test default field definitions."""

    def test_all_field_sets_exist(self):
        expected = [
            "ad_accounts",
            "ad_labels",
            "campaigns",
            "ad_sets",
            "ads",
            "ad_creatives",
            "ad_leads",
            "custom_audiences",
            "custom_conversions",
            "ad_images",
            "ad_videos",
            "activities",
            "saved_audiences",
        ]
        for name in expected:
            assert name in DEFAULT_FIELDS, f"Missing field set: {name}"

    def test_insight_fields_include_basics(self):
        assert "impressions" in DEFAULT_INSIGHT_FIELDS
        assert "clicks" in DEFAULT_INSIGHT_FIELDS
        assert "spend" in DEFAULT_INSIGHT_FIELDS

    def test_insight_fields_include_ids(self):
        assert "campaign_id" in DEFAULT_INSIGHT_FIELDS
        assert "adset_id" in DEFAULT_INSIGHT_FIELDS
        assert "ad_id" in DEFAULT_INSIGHT_FIELDS

    def test_insight_fields_include_dates(self):
        assert "date_start" in DEFAULT_INSIGHT_FIELDS
        assert "date_stop" in DEFAULT_INSIGHT_FIELDS


class TestInsightsPrimaryKeys:
    """Test insights primary key mapping by level."""

    def test_account_level(self):
        assert INSIGHTS_PRIMARY_KEYS["account"] == ["date_start", "date_stop"]

    def test_campaign_level(self):
        assert INSIGHTS_PRIMARY_KEYS["campaign"] == [
            "date_start",
            "date_stop",
            "campaign_id",
        ]

    def test_adset_level(self):
        assert INSIGHTS_PRIMARY_KEYS["adset"] == [
            "date_start",
            "date_stop",
            "adset_id",
        ]

    def test_ad_level(self):
        assert INSIGHTS_PRIMARY_KEYS["ad"] == ["date_start", "date_stop", "ad_id"]


class TestConvertInsightTypes:
    """Test _convert_insight_types() type conversion."""

    def test_converts_int_fields(self):
        row = {"impressions": "1234", "clicks": "56", "reach": "789"}
        result = _convert_insight_types(row)
        assert result["impressions"] == 1234
        assert result["clicks"] == 56
        assert result["reach"] == 789

    def test_converts_float_fields_to_decimal(self):
        row = {"spend": "12.34", "cpc": "0.56", "cpm": "7.89", "ctr": "0.03"}
        result = _convert_insight_types(row)
        assert result["spend"] == Decimal("12.34")
        assert result["cpc"] == Decimal("0.56")
        assert isinstance(result["spend"], Decimal)

    def test_preserves_non_numeric_fields(self):
        row = {"campaign_name": "Test", "impressions": "100"}
        result = _convert_insight_types(row)
        assert result["campaign_name"] == "Test"
        assert result["impressions"] == 100

    def test_handles_none_values(self):
        row = {"impressions": None, "spend": None}
        result = _convert_insight_types(row)
        assert result["impressions"] is None
        assert result["spend"] is None

    def test_handles_missing_fields(self):
        row = {"campaign_name": "Test"}
        result = _convert_insight_types(row)
        assert result == {"campaign_name": "Test"}

    def test_handles_invalid_values(self):
        row = {"impressions": "N/A", "spend": "N/A"}
        result = _convert_insight_types(row)
        assert result["impressions"] == "N/A"
        assert result["spend"] == "N/A"

    def test_int_and_float_field_sets_are_disjoint(self):
        assert INSIGHT_INT_FIELDS.isdisjoint(INSIGHT_FLOAT_FIELDS)

    def test_empty_string_values(self):
        row = {"impressions": "", "spend": ""}
        result = _convert_insight_types(row)
        # empty string cannot be converted, kept as-is
        assert result["impressions"] == ""
        assert result["spend"] == ""


# ---------------------------------------------------------------------------
# Helper: build a mock HTTP response
# ---------------------------------------------------------------------------


def _mock_response(
    status_code: int = 200,
    json_data: dict | None = None,
    headers: dict | None = None,
    raise_for_status_error: bool = False,
):
    """Create a MagicMock that behaves like a requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = json_data or {}
    if raise_for_status_error:
        http_err = req.HTTPError(response=resp)
        resp.raise_for_status.side_effect = http_err
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# _poll_report tests
# ---------------------------------------------------------------------------


class TestPollReport:
    """Test _poll_report() polling logic."""

    def _client(self):
        return _make_client("test_token")

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_completed(self, mock_sleep):
        """Poll returns True when async_status is Job Completed."""
        client = self._client()
        resp = _mock_response(
            json_data={"async_status": "Job Completed", "async_percent_completion": 100}
        )
        client.get = MagicMock(return_value=resp)

        assert _poll_report(client, "run_123", DEFAULT_BASE_URL) is True
        mock_sleep.assert_not_called()

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_429_retry_with_retry_after(self, mock_sleep):
        """429 response triggers wait using Retry-After header, then succeeds."""
        client = self._client()
        err_resp = _mock_response(
            status_code=429,
            headers={"Retry-After": "5"},
            raise_for_status_error=True,
        )
        ok_resp = _mock_response(
            json_data={"async_status": "Job Completed", "async_percent_completion": 100}
        )
        client.get = MagicMock(side_effect=[err_resp, ok_resp])

        assert _poll_report(client, "run_123", DEFAULT_BASE_URL) is True
        mock_sleep.assert_any_call(5)

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_429_retry_exponential_backoff(self, mock_sleep):
        """Multiple 429s use exponential backoff when no Retry-After header."""
        client = self._client()
        err_resp1 = _mock_response(status_code=429, raise_for_status_error=True)
        err_resp2 = _mock_response(status_code=429, raise_for_status_error=True)
        ok_resp = _mock_response(
            json_data={"async_status": "Job Completed", "async_percent_completion": 100}
        )
        client.get = MagicMock(side_effect=[err_resp1, err_resp2, ok_resp])

        assert _poll_report(client, "run_123", DEFAULT_BASE_URL) is True
        # First backoff: POLL_INTERVAL_SECONDS, second: POLL_INTERVAL_SECONDS * 2
        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert sleep_calls[0] == POLL_INTERVAL_SECONDS
        assert sleep_calls[1] == POLL_INTERVAL_SECONDS * 2

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_timeout(self, mock_sleep):
        """Returns False when polling exceeds POLL_MAX_WAIT_SECONDS."""
        client = self._client()
        running_resp = _mock_response(
            json_data={
                "async_status": "Job Running",
                "async_percent_completion": 50,
            }
        )
        client.get = MagicMock(return_value=running_resp)

        result = _poll_report(client, "run_123", DEFAULT_BASE_URL)
        assert result is False
        # Should have polled multiple times
        assert client.get.call_count > 1

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_job_failed(self, mock_sleep):
        """Returns False when async_status is Job Failed."""
        client = self._client()
        resp = _mock_response(
            json_data={"async_status": "Job Failed", "async_percent_completion": 0}
        )
        client.get = MagicMock(return_value=resp)

        assert _poll_report(client, "run_123", DEFAULT_BASE_URL) is False

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_job_skipped(self, mock_sleep):
        """Returns False when async_status is Job Skipped."""
        client = self._client()
        resp = _mock_response(
            json_data={"async_status": "Job Skipped", "async_percent_completion": 0}
        )
        client.get = MagicMock(return_value=resp)

        assert _poll_report(client, "run_123", DEFAULT_BASE_URL) is False

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_non_429_error_raised(self, mock_sleep):
        """Non-429 HTTP errors are re-raised."""
        client = self._client()
        resp = _mock_response(status_code=500, raise_for_status_error=True)
        client.get = MagicMock(return_value=resp)

        with pytest.raises(req.HTTPError):
            _poll_report(client, "run_123", DEFAULT_BASE_URL)


# ---------------------------------------------------------------------------
# _get_paginated tests
# ---------------------------------------------------------------------------


class TestGetPaginated:
    """Test _get_paginated() pagination and error handling."""

    def _client(self):
        return _make_client("test_token")

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_single_page(self, mock_sleep):
        """Single page with no next URL."""
        client = self._client()
        resp = _mock_response(
            json_data={"data": [{"id": "1"}, {"id": "2"}], "paging": {}}
        )
        client.get = MagicMock(return_value=resp)

        results = list(_get_paginated(client, "https://example.com/data"))
        assert results == [{"id": "1"}, {"id": "2"}]
        assert client.get.call_count == 1

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_multiple_pages(self, mock_sleep):
        """Follows paging.next until no more pages."""
        client = self._client()
        page1 = _mock_response(
            json_data={
                "data": [{"id": "1"}],
                "paging": {"next": "https://example.com/data?after=abc"},
            }
        )
        page2 = _mock_response(json_data={"data": [{"id": "2"}], "paging": {}})
        client.get = MagicMock(side_effect=[page1, page2])

        results = list(_get_paginated(client, "https://example.com/data"))
        assert results == [{"id": "1"}, {"id": "2"}]
        assert client.get.call_count == 2

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_403_skip(self, mock_sleep):
        """403 response skips and returns no data."""
        client = self._client()
        resp = _mock_response(status_code=403, raise_for_status_error=True)
        client.get = MagicMock(return_value=resp)

        results = list(_get_paginated(client, "https://example.com/data"))
        assert results == []

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_404_skip(self, mock_sleep):
        """404 response skips and returns no data."""
        client = self._client()
        resp = _mock_response(status_code=404, raise_for_status_error=True)
        client.get = MagicMock(return_value=resp)

        results = list(_get_paginated(client, "https://example.com/data"))
        assert results == []

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_429_retry_then_success(self, mock_sleep):
        """429 triggers retry, then succeeds."""
        client = self._client()
        err_resp = _mock_response(
            status_code=429,
            headers={"Retry-After": "3"},
            raise_for_status_error=True,
        )
        ok_resp = _mock_response(json_data={"data": [{"id": "1"}], "paging": {}})
        client.get = MagicMock(side_effect=[err_resp, ok_resp])

        results = list(_get_paginated(client, "https://example.com/data"))
        assert results == [{"id": "1"}]
        mock_sleep.assert_any_call(3)

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_429_max_retries_exceeded(self, mock_sleep):
        """429 exceeding max_retries raises HTTPError."""
        client = self._client()
        err_resp = _mock_response(status_code=429, raise_for_status_error=True)
        client.get = MagicMock(return_value=err_resp)

        with pytest.raises(req.HTTPError):
            list(_get_paginated(client, "https://example.com/data", max_retries=2))
        # 1 initial + 2 retries = 3 calls
        assert client.get.call_count == 3

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_empty_response(self, mock_sleep):
        """Empty data array returns no items."""
        client = self._client()
        resp = _mock_response(json_data={"data": [], "paging": {}})
        client.get = MagicMock(return_value=resp)

        results = list(_get_paginated(client, "https://example.com/data"))
        assert results == []

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    def test_500_error_raised(self, mock_sleep):
        """Non-retryable errors are raised immediately."""
        client = self._client()
        resp = _mock_response(status_code=500, raise_for_status_error=True)
        client.get = MagicMock(return_value=resp)

        with pytest.raises(req.HTTPError):
            list(_get_paginated(client, "https://example.com/data"))


# ---------------------------------------------------------------------------
# _convert_insight_types additional tests
# ---------------------------------------------------------------------------


class TestConvertInsightTypesExtended:
    """Extended tests for _convert_insight_types() edge cases."""

    def test_zero_int(self):
        row = {"impressions": "0"}
        result = _convert_insight_types(row)
        assert result["impressions"] == 0
        assert isinstance(result["impressions"], int)

    def test_zero_decimal(self):
        row = {"spend": "0.00"}
        result = _convert_insight_types(row)
        assert result["spend"] == Decimal("0.00")

    def test_large_decimal(self):
        row = {"spend": "999999999.999999"}
        result = _convert_insight_types(row)
        assert result["spend"] == Decimal("999999999.999999")

    def test_none_not_converted(self):
        row = {"impressions": None, "spend": None, "cpc": None}
        result = _convert_insight_types(row)
        assert result["impressions"] is None
        assert result["spend"] is None
        assert result["cpc"] is None

    def test_empty_string_not_converted(self):
        row = {"impressions": "", "spend": ""}
        result = _convert_insight_types(row)
        assert result["impressions"] == ""
        assert result["spend"] == ""


# ---------------------------------------------------------------------------
# insights resource logic tests
# ---------------------------------------------------------------------------


class TestInsightsResource:
    """Test insights() resource logic with mocked HTTP calls."""

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_attribution_window_date_calculation(self, mock_make_client, mock_sleep):
        """Verify start date = last_value - attribution_window_days."""
        from dlt_community_sources.meta_ads.source import insights

        client = MagicMock()
        mock_make_client.return_value = client

        # Mock the POST to create async report
        post_resp = _mock_response(json_data={"report_run_id": "run_999"})
        client.post = MagicMock(return_value=post_resp)

        # Mock polling: immediately completed
        poll_resp = _mock_response(
            json_data={"async_status": "Job Completed", "async_percent_completion": 100}
        )
        # Mock insights pages: empty
        pages_resp = _mock_response(json_data={"data": [], "paging": {}})
        client.get = MagicMock(side_effect=[poll_resp, pages_resp])

        # Create the resource with a known last_value
        import dlt

        last_date = dlt.sources.incremental(
            "date_start", initial_value="2024-06-15", row_order="asc"
        )
        resource = insights(
            access_token="token",
            account_id="123",
            level="ad",
            attribution_window_days=28,
            last_date=last_date,
            base_url=DEFAULT_BASE_URL,
        )

        # Consume the generator
        list(resource)

        # Check that the POST was called with correct time_range
        post_call = client.post.call_args
        post_data = post_call.kwargs.get("data") or post_call[1].get("data")
        expected_start = (date(2024, 6, 15) - timedelta(days=28)).isoformat()
        expected_end = (date.today() - timedelta(days=1)).isoformat()
        expected_time_range = f'{{"since":"{expected_start}","until":"{expected_end}"}}'
        assert post_data["time_range"] == expected_time_range

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_report_run_id_extraction(self, mock_make_client, mock_sleep):
        """Extracts report_run_id from POST response."""
        from dlt_community_sources.meta_ads.source import insights

        client = MagicMock()
        mock_make_client.return_value = client

        post_resp = _mock_response(json_data={"report_run_id": "run_abc"})
        client.post = MagicMock(return_value=post_resp)

        poll_resp = _mock_response(
            json_data={"async_status": "Job Completed", "async_percent_completion": 100}
        )
        data_resp = _mock_response(
            json_data={
                "data": [
                    {
                        "date_start": "2024-06-01",
                        "date_stop": "2024-06-01",
                        "impressions": "100",
                        "spend": "5.50",
                    }
                ],
                "paging": {},
            }
        )
        client.get = MagicMock(side_effect=[poll_resp, data_resp])

        import dlt

        resource = insights(
            access_token="token",
            account_id="123",
            level="ad",
            last_date=dlt.sources.incremental(
                "date_start", initial_value="2024-06-01", row_order="asc"
            ),
            base_url=DEFAULT_BASE_URL,
        )
        results = list(resource)

        # The poll was called with the correct report_run_id
        first_get_url = client.get.call_args_list[0].args[0]
        assert "run_abc" in first_get_url

        # Results should have type-converted values
        assert len(results) == 1
        assert results[0]["impressions"] == 100
        assert results[0]["spend"] == Decimal("5.50")

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_no_report_run_id_returns_empty(self, mock_make_client, mock_sleep):
        """Returns nothing when no report_run_id in response."""
        from dlt_community_sources.meta_ads.source import insights

        client = MagicMock()
        mock_make_client.return_value = client

        post_resp = _mock_response(json_data={})
        client.post = MagicMock(return_value=post_resp)

        import dlt

        resource = insights(
            access_token="token",
            account_id="123",
            level="ad",
            last_date=dlt.sources.incremental(
                "date_start", initial_value="2024-06-01", row_order="asc"
            ),
            base_url=DEFAULT_BASE_URL,
        )
        results = list(resource)
        assert results == []


# ---------------------------------------------------------------------------
# ad_leads resource logic tests
# ---------------------------------------------------------------------------


class TestAdLeadsResource:
    """Test ad_leads() resource logic with mocked HTTP calls."""

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_filtering_url_encoding(self, mock_make_client, mock_sleep):
        """Verify filtering parameter is JSON-encoded in the URL."""
        from dlt_community_sources.meta_ads.source import ad_leads

        client = MagicMock()
        mock_make_client.return_value = client

        # First call: list ads (returns one ad)
        ads_resp = _mock_response(json_data={"data": [{"id": "ad_001"}], "paging": {}})
        # Second call: leads for that ad
        leads_resp = _mock_response(
            json_data={
                "data": [
                    {"id": "lead_001", "created_time": "2024-07-01T00:00:00+0000"}
                ],
                "paging": {},
            }
        )
        client.get = MagicMock(side_effect=[ads_resp, leads_resp])

        import dlt

        resource = ad_leads(
            access_token="token",
            account_id="123",
            last_created_time=dlt.sources.incremental(
                "created_time", initial_value="2024-01-01T00:00:00+0000"
            ),
            base_url=DEFAULT_BASE_URL,
        )
        results = list(resource)

        assert len(results) == 1
        assert results[0]["id"] == "lead_001"

        # Check that the leads URL contains the filtering parameter
        leads_call_url = client.get.call_args_list[1].args[0]
        assert "filtering=" in leads_call_url
        # The filtering JSON should contain GREATER_THAN operator
        assert "GREATER_THAN" in leads_call_url

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_incremental_cursor_value(self, mock_make_client, mock_sleep):
        """Uses last_created_time value for filtering via leadgen_forms."""
        from dlt_community_sources.meta_ads.source import ad_leads

        client = MagicMock()
        mock_make_client.return_value = client

        forms_resp = _mock_response(
            json_data={"data": [{"id": "form_001"}], "paging": {}}
        )
        leads_resp = _mock_response(json_data={"data": [], "paging": {}})
        client.get = MagicMock(side_effect=[forms_resp, leads_resp])

        import dlt

        cursor_value = "2024-06-15T12:00:00+0000"
        resource = ad_leads(
            access_token="token",
            account_id="act_456",
            last_created_time=dlt.sources.incremental(
                "created_time", initial_value=cursor_value
            ),
            base_url=DEFAULT_BASE_URL,
        )
        list(resource)

        # Verify the leadgen_forms URL uses the correct account prefix
        forms_call_url = client.get.call_args_list[0].args[0]
        assert "act_456/leadgen_forms" in forms_call_url

        # Verify leads URL uses form_id and filtering with cursor value
        from urllib.parse import unquote

        leads_call_url = unquote(client.get.call_args_list[1].args[0])
        assert "form_001/leads" in leads_call_url
        assert cursor_value in leads_call_url

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_multiple_ads_leads(self, mock_make_client, mock_sleep):
        """Iterates through multiple ads to fetch their leads."""
        from dlt_community_sources.meta_ads.source import ad_leads

        client = MagicMock()
        mock_make_client.return_value = client

        ads_resp = _mock_response(
            json_data={
                "data": [{"id": "ad_001"}, {"id": "ad_002"}],
                "paging": {},
            }
        )
        leads_resp1 = _mock_response(
            json_data={
                "data": [{"id": "lead_1", "created_time": "2024-07-01T00:00:00+0000"}],
                "paging": {},
            }
        )
        leads_resp2 = _mock_response(
            json_data={
                "data": [{"id": "lead_2", "created_time": "2024-07-02T00:00:00+0000"}],
                "paging": {},
            }
        )
        client.get = MagicMock(side_effect=[ads_resp, leads_resp1, leads_resp2])

        import dlt

        resource = ad_leads(
            access_token="token",
            account_id="123",
            last_created_time=dlt.sources.incremental(
                "created_time", initial_value="2024-01-01T00:00:00+0000"
            ),
            base_url=DEFAULT_BASE_URL,
        )
        results = list(resource)

        assert len(results) == 2
        assert results[0]["id"] == "lead_1"
        assert results[1]["id"] == "lead_2"


# ---------------------------------------------------------------------------
# discover_accounts tests
# ---------------------------------------------------------------------------


class TestDiscoverAccounts:
    """Test discover_accounts() helper."""

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_returns_active_accounts(self, mock_make_client, mock_sleep):
        client = MagicMock()
        mock_make_client.return_value = client

        resp = _mock_response(
            json_data={
                "data": [
                    {"id": "act_111", "account_status": 1},
                    {"id": "act_222", "account_status": 2},  # DISABLED
                    {"id": "act_333", "account_status": 1},
                ],
                "paging": {},
            }
        )
        client.get = MagicMock(return_value=resp)

        result = discover_accounts("fake_token")
        assert result == ["act_111", "act_333"]

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_returns_empty_when_no_accounts(self, mock_make_client, mock_sleep):
        client = MagicMock()
        mock_make_client.return_value = client

        resp = _mock_response(json_data={"data": [], "paging": {}})
        client.get = MagicMock(return_value=resp)

        result = discover_accounts("fake_token")
        assert result == []

    @patch("dlt_community_sources.meta_ads.source.time.sleep")
    @patch("dlt_community_sources.meta_ads.source._make_client")
    def test_paginates(self, mock_make_client, mock_sleep):
        client = MagicMock()
        mock_make_client.return_value = client

        page1 = _mock_response(
            json_data={
                "data": [{"id": "act_111", "account_status": 1}],
                "paging": {"next": "https://example.com/page2"},
            }
        )
        page2 = _mock_response(
            json_data={
                "data": [{"id": "act_222", "account_status": 1}],
                "paging": {},
            }
        )
        client.get = MagicMock(side_effect=[page1, page2])

        result = discover_accounts("fake_token")
        assert result == ["act_111", "act_222"]
        assert client.get.call_count == 2
