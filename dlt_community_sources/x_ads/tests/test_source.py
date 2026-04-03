"""Tests for X (Twitter) Ads source configuration and helpers."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from dlt_community_sources.x_ads.source import (
    BASE_URL,
    DEFAULT_METRIC_GROUPS,
    STATS_BATCH_SIZE,
    STATS_MONEY_FIELDS,
    _batch_ids,
    _convert_stats_types,
    _get_paginated,
    x_ads_source,
)


class TestXAdsSource:
    """Test x_ads_source() resource configuration."""

    def setup_method(self):
        with patch(
            "dlt_community_sources.x_ads.source._create_session"
        ) as mock_session:
            mock_session.return_value = MagicMock()
            self.source = x_ads_source(
                consumer_key="test_key",
                consumer_secret="test_secret",
                access_token="test_token",
                access_token_secret="test_token_secret",
                account_id="test_account",
            )

    def test_returns_all_resources(self):
        assert len(self.source) == 12

    def test_resource_names(self):
        names = [r.name for r in self.source]
        expected = [
            "accounts",
            "campaigns",
            "line_items",
            "promoted_tweets",
            "funding_instruments",
            "media_creatives",
            "scheduled_promoted_tweets",
            "tailored_audiences",
            "targeting_criteria",
            "campaign_stats",
            "line_item_stats",
            "promoted_tweet_stats",
        ]
        assert names == expected

    def test_master_data_write_disposition(self):
        master_resources = [
            "accounts",
            "campaigns",
            "line_items",
            "promoted_tweets",
            "funding_instruments",
            "media_creatives",
            "scheduled_promoted_tweets",
            "tailored_audiences",
            "targeting_criteria",
        ]
        for r in self.source:
            if r.name in master_resources:
                assert r.write_disposition == "merge", f"{r.name} should use merge"

    def test_master_data_primary_key(self):
        for r in self.source:
            if r.name in (
                "accounts",
                "campaigns",
                "line_items",
                "promoted_tweets",
                "funding_instruments",
                "media_creatives",
                "scheduled_promoted_tweets",
                "tailored_audiences",
                "targeting_criteria",
            ):
                assert r.compute_table_schema()["columns"]["id"]["primary_key"] is True

    def test_stats_write_disposition(self):
        for r in self.source:
            if r.name.endswith("_stats"):
                assert r.write_disposition == "merge", f"{r.name} should use merge"

    def test_stats_primary_key(self):
        for r in self.source:
            if r.name.endswith("_stats"):
                schema = r.compute_table_schema()
                pk_cols = [
                    col
                    for col, props in schema["columns"].items()
                    if props.get("primary_key")
                ]
                assert "entity_id" in pk_cols
                assert "date" in pk_cols

    def test_stats_date_column_type(self):
        for r in self.source:
            if r.name.endswith("_stats"):
                schema = r.compute_table_schema()
                assert schema["columns"]["date"]["data_type"] == "date"

    def test_filter_resources(self):
        with patch(
            "dlt_community_sources.x_ads.source._create_session"
        ) as mock_session:
            mock_session.return_value = MagicMock()
            source = x_ads_source(
                consumer_key="k",
                consumer_secret="s",
                access_token="t",
                access_token_secret="ts",
                account_id="a",
                resources=["campaigns", "campaign_stats"],
            )
        names = [r.name for r in source]
        assert names == ["campaigns", "campaign_stats"]

    def test_default_base_url(self):
        assert BASE_URL == "https://ads-api.x.com/12"


class TestGetPaginated:
    """Test _get_paginated() cursor pagination."""

    def test_single_page(self):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "data": [{"id": "1"}, {"id": "2"}],
        }
        session.get.return_value = response

        results = list(_get_paginated(session, "https://example.com/api"))
        assert results == [{"id": "1"}, {"id": "2"}]
        assert session.get.call_count == 1

    def test_multiple_pages(self):
        session = MagicMock()
        response1 = MagicMock()
        response1.status_code = 200
        response1.json.return_value = {
            "data": [{"id": "1"}],
            "next_cursor": "cursor_abc",
        }
        response2 = MagicMock()
        response2.status_code = 200
        response2.json.return_value = {
            "data": [{"id": "2"}],
        }
        session.get.side_effect = [response1, response2]

        results = list(_get_paginated(session, "https://example.com/api"))
        assert results == [{"id": "1"}, {"id": "2"}]
        assert session.get.call_count == 2

        # Verify cursor was passed on second call
        second_call_params = session.get.call_args_list[1][1]["params"]
        assert second_call_params["cursor"] == "cursor_abc"

    def test_empty_response(self):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"data": []}
        session.get.return_value = response

        results = list(_get_paginated(session, "https://example.com/api"))
        assert results == []

    def test_404_returns_empty(self):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 404
        session.get.return_value = response

        results = list(_get_paginated(session, "https://example.com/api"))
        assert results == []

    def test_403_returns_empty(self):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 403
        session.get.return_value = response

        results = list(_get_paginated(session, "https://example.com/api"))
        assert results == []

    def test_429_retries(self):
        session = MagicMock()
        rate_limited = MagicMock()
        rate_limited.status_code = 429
        rate_limited.headers = {"Retry-After": "1"}

        success = MagicMock()
        success.status_code = 200
        success.json.return_value = {"data": [{"id": "1"}]}

        session.get.side_effect = [rate_limited, success]

        with patch("dlt_community_sources.x_ads.source.time.sleep"):
            results = list(_get_paginated(session, "https://example.com/api"))

        assert results == [{"id": "1"}]
        assert session.get.call_count == 2

    def test_passes_custom_params(self):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"data": []}
        session.get.return_value = response

        list(
            _get_paginated(
                session,
                "https://example.com/api",
                params={"line_item_ids": "a,b"},
            )
        )

        call_params = session.get.call_args[1]["params"]
        assert call_params["line_item_ids"] == "a,b"
        assert call_params["count"] == 1000


class TestBatchIds:
    """Test _batch_ids() splitting."""

    def test_single_batch(self):
        ids = ["1", "2", "3"]
        batches = _batch_ids(ids, batch_size=20)
        assert batches == [["1", "2", "3"]]

    def test_multiple_batches(self):
        ids = [str(i) for i in range(25)]
        batches = _batch_ids(ids, batch_size=20)
        assert len(batches) == 2
        assert len(batches[0]) == 20
        assert len(batches[1]) == 5

    def test_exact_batch_size(self):
        ids = [str(i) for i in range(20)]
        batches = _batch_ids(ids, batch_size=20)
        assert len(batches) == 1
        assert len(batches[0]) == 20

    def test_empty_list(self):
        batches = _batch_ids([])
        assert batches == []

    def test_default_batch_size(self):
        assert STATS_BATCH_SIZE == 20


class TestConvertStatsTypes:
    """Test _convert_stats_types() type conversion."""

    def test_converts_money_fields(self):
        row = {"billed_charge_local_micro": 5000000}
        result = _convert_stats_types(row)
        assert result["billed_charge_local_micro"] == Decimal("5")
        assert isinstance(result["billed_charge_local_micro"], Decimal)

    def test_preserves_non_money_fields(self):
        row = {"impressions": 100, "clicks": 50, "entity_id": "abc"}
        result = _convert_stats_types(row)
        assert result["impressions"] == 100
        assert result["clicks"] == 50
        assert result["entity_id"] == "abc"

    def test_handles_none_values(self):
        row = {"billed_charge_local_micro": None}
        result = _convert_stats_types(row)
        assert result["billed_charge_local_micro"] is None

    def test_handles_missing_fields(self):
        row = {"entity_id": "test"}
        result = _convert_stats_types(row)
        assert result == {"entity_id": "test"}

    def test_money_fields_defined(self):
        assert "billed_charge_local_micro" in STATS_MONEY_FIELDS


class TestDefaultMetricGroups:
    """Test default metric groups."""

    def test_includes_engagement(self):
        assert "ENGAGEMENT" in DEFAULT_METRIC_GROUPS

    def test_includes_billing(self):
        assert "BILLING" in DEFAULT_METRIC_GROUPS

    def test_includes_video(self):
        assert "VIDEO" in DEFAULT_METRIC_GROUPS
