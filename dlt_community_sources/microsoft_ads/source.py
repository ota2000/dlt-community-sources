"""dlt source for Microsoft Advertising API.

Microsoft Advertising uses POST RPC endpoints. All requests require
Authorization (Bearer), DeveloperToken, CustomerId, and AccountId headers.

Resources are organized by API service:
- Campaign Management (33 resources)
- Customer Management (10 resources)
- Reporting (1 resource, 37 report types)
- Ad Insight (8 resources)
- Customer Billing (3 resources)

SDK reference: https://github.com/BingAds/BingAds-Python-SDK
"""

from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource

from .auth import refresh_access_token
from .resources.ad_insight import ALL_AD_INSIGHT_RESOURCES
from .resources.campaign_management import ALL_CAMPAIGN_MGMT_RESOURCES
from .resources.customer_billing import ALL_CUSTOMER_BILLING_RESOURCES
from .resources.customer_management import ALL_CUSTOMER_MGMT_RESOURCES
from .resources.reporting import report


@dlt.source(name="microsoft_ads")
def microsoft_ads_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    developer_token: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    account_id: str = dlt.secrets.value,
    customer_id: str = dlt.secrets.value,
    report_type: str = "CampaignPerformanceReportRequest",
    report_columns: Optional[list[str]] = None,
    aggregation: str = "Daily",
    attribution_window_days: int = 7,
    resources: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
) -> list[DltResource]:
    """A dlt source for Microsoft Advertising API.

    Covers 55 resources across 5 API services:
    - Campaign Management: campaigns, ad_groups, ads, keywords, etc.
    - Customer Management: account_info, customer_info, users, etc.
    - Reporting: configurable report type with 37 options.
    - Ad Insight: bid/budget/keyword opportunities, recommendations.
    - Customer Billing: monthly spend, billing docs, insertion orders.

    Args:
        client_id: Azure AD app client ID.
        client_secret: Azure AD app client secret.
        developer_token: Microsoft Advertising developer token.
        refresh_token: OAuth refresh token (rotated on each use).
        account_id: Microsoft Advertising account ID.
        customer_id: Microsoft Advertising customer ID.
        report_type: Report type (e.g., CampaignPerformanceReportRequest).
        report_columns: Custom report columns.
        aggregation: Report aggregation (Daily, Weekly, Monthly).
        attribution_window_days: Days to re-fetch for attribution window.
        resources: Resource names to load. None for all.
        start_date: Override incremental start date (YYYY-MM-DD).

    Returns:
        List of dlt resources.
        The caller is responsible for persisting the new refresh_token.
    """
    # Refresh token → access_token (token rotation)
    tokens = refresh_access_token(client_id, client_secret, refresh_token)
    access_token = tokens["access_token"]
    # tokens["refresh_token"] should be persisted by the caller

    auth_args = (access_token, developer_token, customer_id, account_id)

    # Instantiate all resources
    all_resources: list[DltResource] = []

    for resource_fn in ALL_CAMPAIGN_MGMT_RESOURCES:
        all_resources.append(resource_fn(*auth_args))

    for resource_fn in ALL_CUSTOMER_MGMT_RESOURCES:
        all_resources.append(resource_fn(*auth_args))

    for resource_fn in ALL_AD_INSIGHT_RESOURCES:
        all_resources.append(resource_fn(*auth_args))

    for resource_fn in ALL_CUSTOMER_BILLING_RESOURCES:
        all_resources.append(resource_fn(*auth_args))

    # Report resource
    initial_value = start_date or "2020-01-01"
    report_resource = report(
        *auth_args,
        report_type=report_type,
        columns=report_columns,
        aggregation=aggregation,
        attribution_window_days=attribution_window_days,
        last_date=dlt.sources.incremental("TimePeriod", initial_value=initial_value),
    )
    pk = ["TimePeriod", "AccountId"]
    if "Campaign" in report_type:
        pk.append("CampaignId")
    if "AdGroup" in report_type:
        pk.append("AdGroupId")
    if "Ad" in report_type and "AdGroup" not in report_type:
        pk.append("AdId")
    if "Keyword" in report_type:
        pk.append("KeywordId")
    report_resource.apply_hints(primary_key=pk)
    all_resources.append(report_resource)

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources
