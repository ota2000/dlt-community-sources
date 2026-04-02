"""Tests for Microsoft Ads source configuration and helpers."""

import io
import zipfile
from decimal import Decimal
from unittest.mock import MagicMock, patch

from dlt_community_sources.microsoft_ads.resources.ad_insight import (
    ALL_AD_INSIGHT_RESOURCES,
    auction_insight_data,
    auto_apply_opt_in_status,
)
from dlt_community_sources.microsoft_ads.resources.campaign_management import (
    ALL_CAMPAIGN_MGMT_RESOURCES,
)
from dlt_community_sources.microsoft_ads.resources.customer_billing import (
    ALL_CUSTOMER_BILLING_RESOURCES,
    account_monthly_spend,
    billing_documents_info,
)
from dlt_community_sources.microsoft_ads.resources.customer_management import (
    ALL_CUSTOMER_MGMT_RESOURCES,
    account_info,
    customer_pilot_features,
)
from dlt_community_sources.microsoft_ads.resources.helpers import (
    CAMPAIGN_MGMT_URL,
    CUSTOMER_MGMT_URL,
    REPORT_FLOAT_FIELDS,
    REPORT_INT_FIELDS,
    REPORTING_URL,
    build_headers,
    convert_report_types,
    get_entities_paginated,
    make_client,
    post_rpc,
    safe_rpc,
)
from dlt_community_sources.microsoft_ads.resources.reporting import (
    REPORT_COLUMNS,
    REPORT_TYPES,
    _download_csv_report,
    _poll_report,
    _submit_report,
)


class TestBuildHeaders:
    def test_includes_all_required_headers(self):
        headers = build_headers("token", "dev", "cust", "acct")
        assert headers["Authorization"] == "Bearer token"
        assert headers["DeveloperToken"] == "dev"
        assert headers["CustomerId"] == "cust"
        assert headers["CustomerAccountId"] == "acct"

    def test_content_type(self):
        headers = build_headers("t", "d", "c", "a")
        assert headers["Content-Type"] == "application/json"


class TestMakeClient:
    def test_sets_headers(self):
        client = make_client("token", "dev", "cust", "acct")
        assert client.session.headers["Authorization"] == "Bearer token"
        assert client.session.headers["DeveloperToken"] == "dev"


class TestConstants:
    def test_campaign_mgmt_url(self):
        assert "campaign.api.bingads.microsoft.com" in CAMPAIGN_MGMT_URL

    def test_reporting_url(self):
        assert "reporting.api.bingads.microsoft.com" in REPORTING_URL

    def test_customer_mgmt_url(self):
        assert "clientcenter.api.bingads.microsoft.com" in CUSTOMER_MGMT_URL

    def test_report_types_count(self):
        assert len(REPORT_TYPES) >= 35

    def test_report_columns_include_basics(self):
        cols = REPORT_COLUMNS["CampaignPerformanceReportRequest"]
        for field in ["TimePeriod", "CampaignName", "Impressions", "Clicks", "Spend"]:
            assert field in cols

    def test_int_and_float_fields_disjoint(self):
        assert REPORT_INT_FIELDS.isdisjoint(REPORT_FLOAT_FIELDS)


class TestResourceCounts:
    """Verify all API services have resources."""

    def test_campaign_management_resources(self):
        assert len(ALL_CAMPAIGN_MGMT_RESOURCES) >= 30

    def test_customer_management_resources(self):
        assert len(ALL_CUSTOMER_MGMT_RESOURCES) >= 8

    def test_ad_insight_resources(self):
        assert len(ALL_AD_INSIGHT_RESOURCES) >= 5

    def test_customer_billing_resources(self):
        assert len(ALL_CUSTOMER_BILLING_RESOURCES) >= 3


class TestPostRpc:
    def test_returns_json(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Campaigns": [{"Id": 1}]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = post_rpc(mock_client, "http://test", {"key": "val"})
        assert result == {"Campaigns": [{"Id": 1}]}


class TestSafeRpc:
    def test_success(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Items": [{"Id": 1}]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = safe_rpc(mock_client, "http://test", {}, "Items")
        assert result == [{"Id": 1}]

    def test_404_returns_empty(self):
        from dlt.sources.helpers import requests as dlt_req

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_client.post.side_effect = dlt_req.HTTPError(response=mock_resp)
        result = safe_rpc(mock_client, "http://test", {}, "Items")
        assert result == []

    def test_missing_key_returns_empty(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = safe_rpc(mock_client, "http://test", {}, "Items")
        assert result == []


class TestGetEntitiesPaginated:
    def test_single_page(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Items": [{"Id": 1}, {"Id": 2}]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = list(
            get_entities_paginated(
                mock_client, "http://test", {}, "Items", page_size=10
            )
        )
        assert len(result) == 2

    def test_empty_response(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Items": []}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = list(get_entities_paginated(mock_client, "http://test", {}, "Items"))
        assert result == []


class TestConvertReportTypes:
    def test_converts_int_fields(self):
        row = {"Impressions": "1234", "Clicks": "56"}
        result = convert_report_types(row)
        assert result["Impressions"] == 1234

    def test_converts_float_fields(self):
        row = {"Spend": "12.34", "Ctr": "0.03"}
        result = convert_report_types(row)
        assert isinstance(result["Spend"], Decimal)

    def test_handles_none(self):
        row = {"Impressions": None}
        result = convert_report_types(row)
        assert result["Impressions"] is None

    def test_handles_invalid(self):
        row = {"Impressions": "--"}
        result = convert_report_types(row)
        assert result["Impressions"] == "--"


class TestSubmitReport:
    def test_returns_request_id(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ReportRequestId": "abc123"}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _submit_report(
            mock_client,
            "CampaignPerformanceReportRequest",
            "12345",
            ["TimePeriod", "Impressions"],
            "2026-01-01",
            "2026-01-31",
        )
        assert result == "abc123"

    def test_date_parsing(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ReportRequestId": "x"}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        _submit_report(
            mock_client,
            "CampaignPerformanceReportRequest",
            "12345",
            ["TimePeriod"],
            "2026-03-15",
            "2026-04-01",
        )
        call_body = mock_client.post.call_args[1]["json"]
        time_config = call_body["ReportRequest"]["Time"]
        assert time_config["CustomDateRangeStart"] == {
            "Day": 15,
            "Month": 3,
            "Year": 2026,
        }


class TestPollReport:
    def test_success(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "ReportRequestStatus": {
                "Status": "Success",
                "ReportDownloadUrl": "https://download.example.com/report.zip",
            }
        }
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _poll_report(mock_client, "req123")
        assert result == "https://download.example.com/report.zip"

    def test_error_returns_none(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ReportRequestStatus": {"Status": "Error"}}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _poll_report(mock_client, "req123")
        assert result is None

    @patch("dlt_community_sources.microsoft_ads.resources.reporting.time.sleep")
    @patch(
        "dlt_community_sources.microsoft_ads.resources.reporting.POLL_MAX_WAIT_SECONDS",
        20,
    )
    @patch(
        "dlt_community_sources.microsoft_ads.resources.reporting.POLL_INTERVAL_SECONDS",
        10,
    )
    def test_timeout_returns_none(self, mock_sleep):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ReportRequestStatus": {"Status": "Pending"}}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _poll_report(mock_client, "req123")
        assert result is None
        assert mock_sleep.call_count == 2


class TestDownloadCsvReport:
    def test_parses_csv_from_zip(self):
        # Build an in-memory ZIP containing a CSV
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(
                "report.csv",
                "TimePeriod,Impressions,Spend\n2026-01-01,100,12.34\n",
            )
        buf.seek(0)

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = buf.getvalue()
        mock_resp.raise_for_status.return_value = None
        mock_client.get.return_value = mock_resp

        rows = list(_download_csv_report(mock_client, "https://example.com/r.zip"))
        assert len(rows) == 1
        assert rows[0]["TimePeriod"] == "2026-01-01"
        assert rows[0]["Impressions"] == 100
        assert rows[0]["Spend"] == Decimal("12.34")


class TestSourceFunction:
    @patch("dlt_community_sources.microsoft_ads.source.refresh_access_token")
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CAMPAIGN_MGMT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CUSTOMER_MGMT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_AD_INSIGHT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CUSTOMER_BILLING_RESOURCES",
        [],
    )
    def test_returns_report_resource(self, mock_refresh):
        mock_refresh.return_value = {
            "access_token": "at",
            "refresh_token": "rt_new",
        }
        from dlt_community_sources.microsoft_ads.source import microsoft_ads_source

        source = microsoft_ads_source(
            client_id="cid",
            client_secret="cs",
            developer_token="dt",
            refresh_token="rt",
            account_id="aid",
            customer_id="cuid",
        )
        assert "report" in source.resources

    @patch("dlt_community_sources.microsoft_ads.source.refresh_access_token")
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CAMPAIGN_MGMT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CUSTOMER_MGMT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_AD_INSIGHT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CUSTOMER_BILLING_RESOURCES",
        [],
    )
    def test_filter_by_resource_name(self, mock_refresh):
        mock_refresh.return_value = {
            "access_token": "at",
            "refresh_token": "rt_new",
        }
        from dlt_community_sources.microsoft_ads.source import microsoft_ads_source

        source = microsoft_ads_source(
            client_id="cid",
            client_secret="cs",
            developer_token="dt",
            refresh_token="rt",
            account_id="aid",
            customer_id="cuid",
            resources=["report"],
        )
        assert "report" in source.resources

    @patch("dlt_community_sources.microsoft_ads.source.refresh_access_token")
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CAMPAIGN_MGMT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CUSTOMER_MGMT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_AD_INSIGHT_RESOURCES",
        [],
    )
    @patch(
        "dlt_community_sources.microsoft_ads.source.ALL_CUSTOMER_BILLING_RESOURCES",
        [],
    )
    def test_filter_excludes_unknown(self, mock_refresh):
        mock_refresh.return_value = {
            "access_token": "at",
            "refresh_token": "rt_new",
        }
        from dlt_community_sources.microsoft_ads.source import microsoft_ads_source

        source = microsoft_ads_source(
            client_id="cid",
            client_secret="cs",
            developer_token="dt",
            refresh_token="rt",
            account_id="aid",
            customer_id="cuid",
            resources=["nonexistent"],
        )
        # Only resources matching the filter are included
        resource_names = list(source.resources.keys())
        assert "report" not in resource_names


class TestResourceFunctions:
    """Test individual resource functions with mocked clients."""

    @patch("dlt_community_sources.microsoft_ads.resources.ad_insight.make_client")
    def test_auction_insight_data(self, mock_make):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Result": [{"Id": 1}]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        mock_make.return_value = mock_client
        result = list(auction_insight_data("at", "dt", "ci", "ai"))
        assert result == [{"Id": 1}]

    @patch(
        "dlt_community_sources.microsoft_ads.resources.customer_management.make_client"
    )
    def test_account_info(self, mock_make):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Account": {"Id": 1, "Name": "Test"}}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        mock_make.return_value = mock_client
        result = list(account_info("at", "dt", "ci", "ai"))
        assert result == [{"Id": 1, "Name": "Test"}]

    @patch(
        "dlt_community_sources.microsoft_ads.resources.customer_management.make_client"
    )
    def test_customer_pilot_features(self, mock_make):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"FeaturePilotFlags": [100, 200]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        mock_make.return_value = mock_client
        result = list(customer_pilot_features("at", "dt", "ci", "ai"))
        assert len(result) == 2
        assert result[0]["feature_id"] == 100

    @patch("dlt_community_sources.microsoft_ads.resources.ad_insight.make_client")
    def test_auto_apply_opt_in_status(self, mock_make):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "AutoApplyRecommendationsStatus": [
                {
                    "AAROptInStatus": "OptedIn",
                    "RecommendationType": "ResponsiveSearchAdsOpportunity",
                },
                {
                    "AAROptInStatus": "OptedOut",
                    "RecommendationType": "MultiMediaAdsOpportunity",
                },
            ]
        }
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        mock_make.return_value = mock_client
        result = list(auto_apply_opt_in_status("at", "dt", "ci", "ai"))
        assert len(result) == 2
        assert result[0]["AAROptInStatus"] == "OptedIn"

    @patch("dlt_community_sources.microsoft_ads.resources.customer_billing.make_client")
    def test_account_monthly_spend(self, mock_make):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Amount": 1234.56}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        mock_make.return_value = mock_client
        result = list(account_monthly_spend("at", "dt", "ci", "ai"))
        assert result[0]["amount"] == 1234.56

    @patch("dlt_community_sources.microsoft_ads.resources.customer_billing.make_client")
    def test_billing_documents_info(self, mock_make):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"BillingDocumentsInfo": [{"DocumentId": "doc1"}]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        mock_make.return_value = mock_client
        result = list(billing_documents_info("at", "dt", "ci", "ai"))
        assert result == [{"DocumentId": "doc1"}]
