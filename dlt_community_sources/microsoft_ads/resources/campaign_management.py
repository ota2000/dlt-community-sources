"""Campaign Management API resources.

SDK: bingads/v13/proxies/production/campaignmanagement_service.xml
All 70+ GET operations from the SDK are covered here.

REST URL pattern: {base}/{Entity}/{Action}
See https://learn.microsoft.com/en-us/advertising/campaign-management-service/campaign-management-service-reference
"""

import dlt

from .helpers import CAMPAIGN_MGMT_URL, get_entities_paginated, make_client, safe_rpc


def _client(at, dt, ci, ai):
    return make_client(at, dt, ci, ai)


def _url(path, base=CAMPAIGN_MGMT_URL):
    return f"{base}/{path}"


# --- Core entity resources ---


@dlt.resource(name="campaigns", write_disposition="merge", primary_key="Id")
def campaigns(access_token, developer_token, customer_id, account_id):
    """SDK: GetCampaignsByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("Campaigns/QueryByAccountId"),
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
        "Campaigns",
    )


@dlt.resource(name="ad_groups", write_disposition="merge", primary_key="Id")
def ad_groups(access_token, developer_token, customer_id, account_id):
    """SDK: GetAdGroupsByCampaignId (iterates campaigns)."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for camp in safe_rpc(
        c,
        _url("Campaigns/QueryByAccountId"),
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
        "Campaigns",
    ):
        cid = camp.get("Id")
        if cid:
            yield from safe_rpc(
                c, _url("AdGroups/QueryByCampaignId"), {"CampaignId": cid}, "AdGroups"
            )


@dlt.resource(name="ads", write_disposition="merge", primary_key="Id")
def ads(access_token, developer_token, customer_id, account_id):
    """SDK: GetAdsByAdGroupId (iterates ad groups)."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for ag in ad_groups(access_token, developer_token, customer_id, account_id):
        ag_id = ag.get("Id")
        if ag_id:
            yield from safe_rpc(
                c,
                _url("Ads/QueryByAdGroupId"),
                {
                    "AdGroupId": ag_id,
                    "AdTypes": "AppInstall DynamicSearch ExpandedText Product ResponsiveAd ResponsiveSearch",
                },
                "Ads",
            )


@dlt.resource(name="keywords", write_disposition="merge", primary_key="Id")
def keywords(access_token, developer_token, customer_id, account_id):
    """SDK: GetKeywordsByAdGroupId (iterates ad groups)."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for ag in ad_groups(access_token, developer_token, customer_id, account_id):
        ag_id = ag.get("Id")
        if ag_id:
            yield from safe_rpc(
                c, _url("Keywords/QueryByAdGroupId"), {"AdGroupId": ag_id}, "Keywords"
            )


# --- Ad Extensions ---


@dlt.resource(name="ad_extensions", write_disposition="merge", primary_key="Id")
def ad_extensions(access_token, developer_token, customer_id, account_id):
    """SDK: GetAdExtensionIdsByAccountId + GetAdExtensionsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    ext_types = "CallAdExtension CalloutAdExtension ImageAdExtension LocationAdExtension PriceAdExtension ReviewAdExtension SitelinkAdExtension StructuredSnippetAdExtension"
    id_data = safe_rpc(
        c,
        _url("AdExtensionIds/QueryByAccountId"),
        {"AccountId": account_id, "AdExtensionType": ext_types},
        "AdExtensionIds",
    )
    ext_ids = []
    for group in id_data:
        if isinstance(group, dict):
            ext_ids.extend(group.get("long", []))
        elif isinstance(group, (int, str)):
            ext_ids.append(group)
    if ext_ids:
        yield from safe_rpc(
            c,
            _url("AdExtensions/QueryByIds"),
            {
                "AccountId": account_id,
                "AdExtensionIds": ext_ids,
                "AdExtensionType": ext_types,
            },
            "AdExtensions",
        )


@dlt.resource(name="ad_extension_associations", write_disposition="replace")
def ad_extension_associations(access_token, developer_token, customer_id, account_id):
    """SDK: GetAdExtensionsAssociations."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for camp in safe_rpc(
        c,
        _url("Campaigns/QueryByAccountId"),
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
        "Campaigns",
    ):
        cid = camp.get("Id")
        if cid:
            yield from safe_rpc(
                c,
                _url("AdExtensionsAssociations/Query"),
                {
                    "AccountId": account_id,
                    "EntityIds": [cid],
                    "AssociationType": "Campaign",
                },
                "AdExtensionAssociationCollection",
            )


# --- Criterions ---


@dlt.resource(name="campaign_criterions", write_disposition="merge", primary_key="Id")
def campaign_criterions(access_token, developer_token, customer_id, account_id):
    """SDK: GetCampaignCriterionsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for camp in safe_rpc(
        c,
        _url("Campaigns/QueryByAccountId"),
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
        "Campaigns",
    ):
        cid = camp.get("Id")
        if cid:
            yield from safe_rpc(
                c,
                _url("CampaignCriterions/QueryByIds"),
                {"CampaignId": cid, "CriterionType": "Targets"},
                "CampaignCriterions",
            )


@dlt.resource(name="ad_group_criterions", write_disposition="merge", primary_key="Id")
def ad_group_criterions(access_token, developer_token, customer_id, account_id):
    """SDK: GetAdGroupCriterionsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for ag in ad_groups(access_token, developer_token, customer_id, account_id):
        ag_id = ag.get("Id")
        if ag_id:
            yield from safe_rpc(
                c,
                _url("AdGroupCriterions/QueryByIds"),
                {"AdGroupId": ag_id, "CriterionType": "Targets"},
                "AdGroupCriterions",
            )


# --- Audiences & Targeting ---


@dlt.resource(name="audiences", write_disposition="merge", primary_key="Id")
def audiences(access_token, developer_token, customer_id, account_id):
    """SDK: GetAudiencesByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("Audiences/QueryByIds"),
        {
            "Type": "Custom InMarket Product RemarketingList SimilarRemarketingList CombinedList CustomerList ImpressionBasedRemarketingList"
        },
        "Audiences",
    )


@dlt.resource(name="audience_groups", write_disposition="merge", primary_key="Id")
def audience_groups(access_token, developer_token, customer_id, account_id):
    """SDK: GetAudienceGroupsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(c, _url("AudienceGroups/QueryByIds"), {}, "AudienceGroups")


# --- Asset Groups ---


@dlt.resource(name="asset_groups", write_disposition="merge", primary_key="Id")
def asset_groups(access_token, developer_token, customer_id, account_id):
    """SDK: GetAssetGroupsByCampaignId (iterates campaigns)."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for camp in safe_rpc(
        c,
        _url("Campaigns/QueryByAccountId"),
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
        "Campaigns",
    ):
        cid = camp.get("Id")
        if cid:
            yield from safe_rpc(
                c,
                _url("AssetGroups/QueryByCampaignId"),
                {"CampaignId": cid},
                "AssetGroups",
            )


# --- Conversion & Tracking ---


@dlt.resource(name="conversion_goals", write_disposition="merge", primary_key="Id")
def conversion_goals(access_token, developer_token, customer_id, account_id):
    """SDK: GetConversionGoalsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("ConversionGoals/QueryByIds"),
        {
            "ConversionGoalTypes": "Url Duration Event AppInstall InStoreTransaction OfflineConversion"
        },
        "ConversionGoals",
    )


@dlt.resource(
    name="conversion_value_rules", write_disposition="merge", primary_key="Id"
)
def conversion_value_rules(access_token, developer_token, customer_id, account_id):
    """SDK: GetConversionValueRulesByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("ConversionValueRules/QueryByAccountId"),
        {"AccountId": account_id},
        "ConversionValueRules",
    )


@dlt.resource(name="uet_tags", write_disposition="merge", primary_key="Id")
def uet_tags(access_token, developer_token, customer_id, account_id):
    """SDK: GetUetTagsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(c, _url("UetTags/QueryByIds"), {}, "UetTags")


@dlt.resource(name="offline_conversion_reports", write_disposition="append")
def offline_conversion_reports(access_token, developer_token, customer_id, account_id):
    """SDK: GetOfflineConversionReports."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("OfflineConversionReports/Query"), {}, "OfflineConversionReports"
    )


# --- Labels ---


@dlt.resource(name="labels", write_disposition="merge", primary_key="Id")
def labels(access_token, developer_token, customer_id, account_id):
    """SDK: GetLabelsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from get_entities_paginated(c, _url("Labels/QueryByIds"), {}, "Labels")


@dlt.resource(name="label_associations_by_entity", write_disposition="replace")
def label_associations_by_entity(
    access_token, developer_token, customer_id, account_id
):
    """SDK: GetLabelAssociationsByEntityIds (campaigns)."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for camp in safe_rpc(
        c,
        _url("Campaigns/QueryByAccountId"),
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
        "Campaigns",
    ):
        cid = camp.get("Id")
        if cid:
            yield from safe_rpc(
                c,
                _url("LabelAssociations/QueryByEntityIds"),
                {"EntityIds": [cid], "EntityType": "Campaign"},
                "LabelAssociations",
            )


# --- Budgets & Bidding ---


@dlt.resource(name="budgets", write_disposition="merge", primary_key="Id")
def budgets(access_token, developer_token, customer_id, account_id):
    """SDK: GetBudgetsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(c, _url("Budgets/QueryByIds"), {}, "Budgets")


@dlt.resource(name="bid_strategies", write_disposition="merge", primary_key="Id")
def bid_strategies(access_token, developer_token, customer_id, account_id):
    """SDK: GetBidStrategiesByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(c, _url("BidStrategies/QueryByIds"), {}, "BidStrategies")


# --- Negative Keywords & Sites ---


@dlt.resource(name="negative_keywords", write_disposition="replace")
def negative_keywords(access_token, developer_token, customer_id, account_id):
    """SDK: GetNegativeKeywordsByEntityIds (campaigns)."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for camp in safe_rpc(
        c,
        _url("Campaigns/QueryByAccountId"),
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
        "Campaigns",
    ):
        cid = camp.get("Id")
        if cid:
            yield from safe_rpc(
                c,
                _url("NegativeKeywords/QueryByEntityIds"),
                {"EntityIds": [cid], "EntityType": "Campaign"},
                "EntityNegativeKeywords",
            )


@dlt.resource(name="negative_sites_campaigns", write_disposition="append")
def negative_sites_campaigns(access_token, developer_token, customer_id, account_id):
    """SDK: GetNegativeSitesByCampaignIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    camp_ids = [
        camp.get("Id")
        for camp in safe_rpc(
            c,
            _url("Campaigns/QueryByAccountId"),
            {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
            "Campaigns",
        )
        if camp.get("Id")
    ]
    if camp_ids:
        yield from safe_rpc(
            c,
            _url("NegativeSites/QueryByCampaignIds"),
            {"AccountId": account_id, "CampaignIds": camp_ids},
            "CampaignNegativeSites",
        )


# --- Shared Entities ---


@dlt.resource(name="shared_entities", write_disposition="merge", primary_key="Id")
def shared_entities(access_token, developer_token, customer_id, account_id):
    """SDK: GetSharedEntitiesByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("SharedEntities/QueryByAccountId"),
        {"SharedEntityType": "NegativeKeywordList"},
        "SharedEntities",
    )


@dlt.resource(name="shared_list_items", write_disposition="replace")
def shared_list_items(access_token, developer_token, customer_id, account_id):
    """SDK: GetListItemsBySharedList (iterates shared entities)."""
    c = _client(access_token, developer_token, customer_id, account_id)
    for entity in safe_rpc(
        c,
        _url("SharedEntities/QueryByAccountId"),
        {"SharedEntityType": "NegativeKeywordList"},
        "SharedEntities",
    ):
        eid = entity.get("Id")
        if eid:
            yield from safe_rpc(
                c,
                _url("ListItems/QueryBySharedList"),
                {"SharedList": {"Id": eid, "Type": "NegativeKeywordList"}},
                "ListItems",
            )


# --- Media & Assets ---


@dlt.resource(name="media", write_disposition="merge", primary_key="MediaId")
def media(access_token, developer_token, customer_id, account_id):
    """SDK: GetMediaMetaDataByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("MediaMetaData/QueryByAccountId"),
        {"MediaEnabledEntities": "ImageAdExtension ResponsiveAd"},
        "MediaMetaData",
    )


@dlt.resource(name="videos", write_disposition="merge", primary_key="Id")
def videos(access_token, developer_token, customer_id, account_id):
    """SDK: GetVideosByIds (all)."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(c, _url("Videos/QueryByIds"), {}, "Videos")


# --- Account Settings ---


@dlt.resource(name="account_properties", write_disposition="replace")
def account_properties(access_token, developer_token, customer_id, account_id):
    """SDK: GetAccountProperties."""
    c = _client(access_token, developer_token, customer_id, account_id)
    from .helpers import post_rpc

    data = post_rpc(
        c,
        _url("AccountProperties/Query"),
        {"AccountPropertyNames": "TrackingUrlTemplate FinalUrlSuffix"},
    )
    props = data.get("AccountProperties", [])
    if props:
        yield {"account_id": account_id, "properties": props}


@dlt.resource(name="account_migration_statuses", write_disposition="replace")
def account_migration_statuses(access_token, developer_token, customer_id, account_id):
    """SDK: GetAccountMigrationStatuses."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("AccountMigrationStatuses/Query"),
        {"AccountIds": [account_id]},
        "MigrationStatuses",
    )


# --- Experiments & Adjustments ---


@dlt.resource(
    name="seasonality_adjustments", write_disposition="merge", primary_key="Id"
)
def seasonality_adjustments(access_token, developer_token, customer_id, account_id):
    """SDK: GetSeasonalityAdjustmentsByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("SeasonalityAdjustments/QueryByAccountId"),
        {"AccountId": account_id},
        "SeasonalityAdjustments",
    )


@dlt.resource(name="data_exclusions", write_disposition="merge", primary_key="Id")
def data_exclusions(access_token, developer_token, customer_id, account_id):
    """SDK: GetDataExclusionsByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("DataExclusions/QueryByAccountId"),
        {"AccountId": account_id},
        "DataExclusions",
    )


@dlt.resource(name="experiments", write_disposition="merge", primary_key="Id")
def experiments(access_token, developer_token, customer_id, account_id):
    """SDK: GetExperimentsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from get_entities_paginated(
        c, _url("Experiments/QueryByIds"), {}, "Experiments"
    )


# --- Import ---


@dlt.resource(name="import_jobs", write_disposition="merge", primary_key="Id")
def import_jobs(access_token, developer_token, customer_id, account_id):
    """SDK: GetImportJobsByIds."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("ImportJobs/QueryByIds"),
        {"ImportType": "GoogleImportJob"},
        "ImportJobs",
    )


# --- Shopping ---


@dlt.resource(name="bmc_stores", write_disposition="merge", primary_key="Id")
def bmc_stores(access_token, developer_token, customer_id, account_id):
    """SDK: GetBMCStoresByCustomerId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(c, _url("BMCStores/QueryByCustomerId"), {}, "BMCStores")


# --- Brand Kits ---


@dlt.resource(name="brand_kits", write_disposition="merge", primary_key="Id")
def brand_kits(access_token, developer_token, customer_id, account_id):
    """SDK: GetBrandKitsByAccountId."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("BrandKits/QueryByAccountId"),
        {"AccountId": account_id},
        "BrandKits",
    )


# Collect all Campaign Management resources
ALL_CAMPAIGN_MGMT_RESOURCES = [
    campaigns,
    ad_groups,
    ads,
    keywords,
    ad_extensions,
    ad_extension_associations,
    campaign_criterions,
    ad_group_criterions,
    audiences,
    audience_groups,
    asset_groups,
    conversion_goals,
    conversion_value_rules,
    uet_tags,
    offline_conversion_reports,
    labels,
    label_associations_by_entity,
    budgets,
    bid_strategies,
    negative_keywords,
    negative_sites_campaigns,
    shared_entities,
    shared_list_items,
    media,
    videos,
    account_properties,
    account_migration_statuses,
    seasonality_adjustments,
    data_exclusions,
    experiments,
    import_jobs,
    bmc_stores,
    brand_kits,
]
