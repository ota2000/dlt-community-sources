"""Ad Insight API resources.

SDK: bingads/v13/proxies/production/adinsight_service.xml
"""

import dlt

from .helpers import AD_INSIGHT_URL, make_client, safe_rpc


def _client(at, dt, ci, ai):
    return make_client(at, dt, ci, ai)


def _url(op, base=AD_INSIGHT_URL):
    return f"{base}/{op}"


@dlt.resource(name="auction_insight_data", write_disposition="replace")
def auction_insight_data(access_token, developer_token, customer_id, account_id):
    """SDK: GetAuctionInsightData."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("GetAuctionInsightData"),
        {"EntityType": "Account", "EntityIds": [account_id]},
        "Result",
    )


@dlt.resource(name="bid_opportunities", write_disposition="replace")
def bid_opportunities(access_token, developer_token, customer_id, account_id):
    """SDK: GetBidOpportunities."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("GetBidOpportunities"), {"AccountId": account_id}, "Opportunities"
    )


@dlt.resource(name="budget_opportunities", write_disposition="replace")
def budget_opportunities(access_token, developer_token, customer_id, account_id):
    """SDK: GetBudgetOpportunities."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("GetBudgetOpportunities"), {"AccountId": account_id}, "Opportunities"
    )


@dlt.resource(name="keyword_opportunities", write_disposition="replace")
def keyword_opportunities(access_token, developer_token, customer_id, account_id):
    """SDK: GetKeywordOpportunities."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("GetKeywordOpportunities"), {"AccountId": account_id}, "Opportunities"
    )


@dlt.resource(name="recommendations", write_disposition="replace")
def recommendations(access_token, developer_token, customer_id, account_id):
    """SDK: GetRecommendations."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("GetRecommendations"), {"AccountId": account_id}, "Recommendations"
    )


@dlt.resource(name="performance_insights", write_disposition="replace")
def performance_insights(access_token, developer_token, customer_id, account_id):
    """SDK: GetPerformanceInsightsDetailDataByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("GetPerformanceInsightsDetailDataByAccountId"),
        {"AccountId": account_id},
        "PerformanceInsights",
    )


@dlt.resource(name="keyword_ideas", write_disposition="replace")
def keyword_ideas(access_token, developer_token, customer_id, account_id):
    """SDK: GetKeywordIdeaCategories."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("GetKeywordIdeaCategories"), {}, "KeywordIdeaCategories"
    )


ALL_AD_INSIGHT_RESOURCES = [
    auction_insight_data,
    bid_opportunities,
    budget_opportunities,
    keyword_opportunities,
    recommendations,
    performance_insights,
    keyword_ideas,
]
