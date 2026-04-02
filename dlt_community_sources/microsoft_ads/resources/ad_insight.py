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


@dlt.resource(name="auction_insight_data", write_disposition="replace")
def auction_insight_data(access_token, developer_token, customer_id, account_id):
    """SDK: GetAuctionInsightData."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("AuctionInsightData/Query"),
        {"EntityType": "Account", "EntityIds": [account_id]},
        "Result",
    )


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
        {"AccountId": account_id},
        "Opportunities",
    )


@dlt.resource(name="recommendations", write_disposition="replace")
def recommendations(access_token, developer_token, customer_id, account_id):
    """SDK: GetRecommendations."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("Recommendations/Query"),
        {"AccountId": account_id},
        "Recommendations",
    )


@dlt.resource(name="performance_insights", write_disposition="replace")
def performance_insights(access_token, developer_token, customer_id, account_id):
    """SDK: GetPerformanceInsightsDetailDataByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("PerformanceInsightsDetailData/QueryByAccountId"),
        {"AccountId": account_id},
        "PerformanceInsights",
    )


@dlt.resource(name="keyword_idea_categories", write_disposition="replace")
def keyword_idea_categories(access_token, developer_token, customer_id, account_id):
    """SDK: GetKeywordIdeaCategories."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("KeywordIdeaCategories/Query"), {}, "KeywordIdeaCategories"
    )


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
    auction_insight_data,
    bid_opportunities,
    budget_opportunities,
    keyword_opportunities,
    recommendations,
    performance_insights,
    keyword_idea_categories,
    auto_apply_opt_in_status,
]
