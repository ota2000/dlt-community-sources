"""Ad Insight API resources.

SDK: bingads/v13/proxies/production/adinsight_service.xml

REST URL pattern: {base}/{Entity}/{Action}
See https://learn.microsoft.com/en-us/advertising/ad-insight-service/ad-insight-service-reference
"""

import dlt

from .helpers import AD_INSIGHT_URL, make_client, safe_rpc


def _client(at, dt, ci, ai):
    return make_client(at, dt, ci, ai)


def _url(path, base=AD_INSIGHT_URL):
    return f"{base}/{path}"


@dlt.resource(name="bid_opportunities", write_disposition="replace")
def bid_opportunities(access_token, developer_token, customer_id, account_id):
    """SDK: GetBidOpportunities."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("BidOpportunities/Query"),
        {"AccountId": account_id},
        "Opportunities",
    )


@dlt.resource(name="budget_opportunities", write_disposition="replace")
def budget_opportunities(access_token, developer_token, customer_id, account_id):
    """SDK: GetBudgetOpportunities."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("BudgetOpportunities/Query"),
        {"AccountId": account_id},
        "Opportunities",
    )


@dlt.resource(name="keyword_opportunities", write_disposition="replace")
def keyword_opportunities(access_token, developer_token, customer_id, account_id):
    """SDK: GetKeywordOpportunities."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("KeywordOpportunities/Query"),
        {"AccountId": account_id, "OpportunityType": "BroadMatch"},
        "Opportunities",
    )


# NOTE: The following resources have been removed because they require
# analysis-specific input parameters that cannot be generalized:
# - auction_insight_data: requires SearchParameters with specific keywords/URLs
# - recommendations: requires specific RecommendationType selection
# - performance_insights: requires specific EntityType and date format
# - keyword_idea_categories: endpoint deprecated by Microsoft


# All known recommendation types for auto-apply opt-in status
_AUTO_APPLY_RECOMMENDATION_TYPES = [
    "ResponsiveSearchAdsOpportunity",
    "MultiMediaAdsOpportunity",
    "RemoveConflictingNegativeKeywordOpportunity",
    "FixConversionGoalSettingsOpportunity",
    "CreateConversionGoalOpportunity",
]


@dlt.resource(name="auto_apply_opt_in_status", write_disposition="replace")
def auto_apply_opt_in_status(access_token, developer_token, customer_id, account_id):
    """SDK: GetAutoApplyOptInStatus."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("AutoApplyOptInStatus/Query"),
        {"RecommendationTypesInputs": _AUTO_APPLY_RECOMMENDATION_TYPES},
        "AutoApplyRecommendationsStatus",
    )


ALL_AD_INSIGHT_RESOURCES = [
    bid_opportunities,
    budget_opportunities,
    keyword_opportunities,
    auto_apply_opt_in_status,
]
