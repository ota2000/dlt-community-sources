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

import logging
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource

from dlt_community_sources._utils import wrap_resources_safe

from .resources.ad_insight import ALL_AD_INSIGHT_RESOURCES
from .resources.campaign_management import ALL_CAMPAIGN_MGMT_RESOURCES
from .resources.customer_billing import ALL_CUSTOMER_BILLING_RESOURCES
from .resources.customer_management import ALL_CUSTOMER_MGMT_RESOURCES
from .resources.helpers import CUSTOMER_MGMT_URL, post_rpc
from .resources.reporting import report

logger = logging.getLogger(__name__)


def discover_accounts(
    access_token: str,
    developer_token: str,
    customer_id: str = "",
) -> list[dict]:
    """Discover all ad accounts accessible by the token.

    Returns a list of dicts with 'Id', 'Name', 'Number',
    'AccountLifeCycleStatus' for each account.

    Only Active accounts are included by default. Paused/Suspended
    accounts are excluded. To access them, pass their account_id
    directly to the source.
    """
    from .resources.helpers import make_client

    client = make_client(access_token, developer_token, customer_id, "")
    data = post_rpc(
        client,
        f"{CUSTOMER_MGMT_URL}/AccountsInfo/Query",
        {},
    )
    accounts = data.get("AccountsInfo") or []
    return [
        {"id": str(a["Id"]), "name": a.get("Name", ""), "number": a.get("Number", "")}
        for a in accounts
        if a.get("AccountLifeCycleStatus") == "Active"
    ]


@dlt.source(name="microsoft_ads")
def microsoft_ads_source(
    access_token: str = dlt.secrets.value,
    developer_token: str = dlt.secrets.value,
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

    Token management is the caller's responsibility. Use
    ``refresh_access_token()`` from ``dlt_community_sources.microsoft_ads``
    to obtain a fresh ``access_token`` before calling this source.
    Microsoft rotates refresh_tokens on each use, so the caller must
    persist the new refresh_token (e.g., to Secret Manager).

    Args:
        access_token: Microsoft access token (obtained via refresh_access_token).
        developer_token: Microsoft Advertising developer token.
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
    """

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

    all_resources = wrap_resources_safe(all_resources)

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources
