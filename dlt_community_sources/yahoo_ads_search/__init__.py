"""Yahoo Ads Search (SS) source for dlt."""

from dlt_community_sources.yahoo_ads_common import refresh_access_token

from .source import yahoo_ads_search_source

__all__ = ["yahoo_ads_search_source", "refresh_access_token"]
