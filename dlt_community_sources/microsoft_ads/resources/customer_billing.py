"""Customer Billing API resources.

SDK: bingads/v13/proxies/production/customerbilling_service.xml

REST URL pattern: {base}/{Entity}/{Action}
See https://learn.microsoft.com/en-us/advertising/customer-billing-service/customer-billing-service-reference
"""

from datetime import date

import dlt

from .helpers import CUSTOMER_BILLING_URL, make_client, post_rpc, safe_rpc


def _client(at, dt, ci, ai):
    return make_client(at, dt, ci, ai)


def _url(path, base=CUSTOMER_BILLING_URL):
    return f"{base}/{path}"


@dlt.resource(
    name="account_monthly_spend", write_disposition="merge", primary_key="account_id"
)
def account_monthly_spend(access_token, developer_token, customer_id, account_id):
    """SDK: GetAccountMonthlySpend."""
    c = _client(access_token, developer_token, customer_id, account_id)
    month = date.today().replace(day=1).isoformat() + "T00:00:00"
    data = post_rpc(
        c,
        _url("AccountMonthlySpend/Query"),
        {"AccountId": account_id, "MonthYear": month},
    )
    amount = data.get("Amount")
    if amount is not None:
        yield {"account_id": account_id, "amount": amount}


@dlt.resource(name="billing_documents_info", write_disposition="append")
def billing_documents_info(access_token, developer_token, customer_id, account_id):
    """SDK: GetBillingDocumentsInfo."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("BillingDocumentsInfo/Query"),
        {
            "AccountIds": [account_id],
            "StartDate": "2020-01-01T00:00:00",
            "EndDate": date.today().isoformat() + "T00:00:00",
        },
        "BillingDocumentsInfo",
    )


@dlt.resource(name="insertion_orders", write_disposition="merge", primary_key="Id")
def insertion_orders(access_token, developer_token, customer_id, account_id):
    """SDK: SearchInsertionOrders."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("InsertionOrders/Search"),
        {
            "Predicates": [
                {
                    "Field": "AccountId",
                    "Operator": "Equals",
                    "Value": str(account_id),
                }
            ],
            "Ordering": [],
            "PageInfo": {"Index": 0, "Size": 1000},
        },
        "InsertionOrders",
    )


ALL_CUSTOMER_BILLING_RESOURCES = [
    account_monthly_spend,
    billing_documents_info,
    insertion_orders,
]
