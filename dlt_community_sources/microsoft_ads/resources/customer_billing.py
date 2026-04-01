"""Customer Billing API resources.

SDK: bingads/v13/proxies/production/customerbilling_service.xml
"""

import dlt

from .helpers import CUSTOMER_BILLING_URL, make_client, post_rpc, safe_rpc


def _client(at, dt, ci, ai):
    return make_client(at, dt, ci, ai)


def _url(op, base=CUSTOMER_BILLING_URL):
    return f"{base}/{op}"


@dlt.resource(name="account_monthly_spend", write_disposition="replace")
def account_monthly_spend(access_token, developer_token, customer_id, account_id):
    """SDK: GetAccountMonthlySpend."""
    c = _client(access_token, developer_token, customer_id, account_id)
    data = post_rpc(c, _url("GetAccountMonthlySpend"), {"AccountId": account_id})
    amount = data.get("Amount")
    if amount is not None:
        yield {"account_id": account_id, "amount": amount}


@dlt.resource(name="billing_documents_info", write_disposition="append")
def billing_documents_info(access_token, developer_token, customer_id, account_id):
    """SDK: GetBillingDocumentsInfo."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("GetBillingDocumentsInfo"),
        {"AccountIds": [account_id]},
        "BillingDocumentsInfo",
    )


@dlt.resource(name="insertion_orders", write_disposition="merge", primary_key="Id")
def insertion_orders(access_token, developer_token, customer_id, account_id):
    """SDK: SearchInsertionOrders."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("SearchInsertionOrders"),
        {
            "Predicates": [
                {"Field": "AccountId", "Operator": "Equals", "Value": account_id}
            ]
        },
        "InsertionOrders",
    )


ALL_CUSTOMER_BILLING_RESOURCES = [
    account_monthly_spend,
    billing_documents_info,
    insertion_orders,
]
