"""Customer Management API resources.

SDK: bingads/v13/proxies/production/customermanagement_service.xml

REST URL pattern: {base}/{Entity}/{Action}
See https://learn.microsoft.com/en-us/advertising/customer-management-service/customer-management-service-reference
"""

import dlt

from .helpers import CUSTOMER_MGMT_URL, make_client, post_rpc, safe_rpc


def _client(at, dt, ci, ai):
    return make_client(at, dt, ci, ai)


def _url(path, base=CUSTOMER_MGMT_URL):
    return f"{base}/{path}"


@dlt.resource(name="account_info", write_disposition="merge", primary_key="Id")
def account_info(access_token, developer_token, customer_id, account_id):
    """SDK: GetAccount."""
    c = _client(access_token, developer_token, customer_id, account_id)
    data = post_rpc(c, _url("Account/Query"), {"AccountId": account_id})
    account = data.get("Account")
    if account:
        yield account


@dlt.resource(name="accounts_info", write_disposition="merge", primary_key="Id")
def accounts_info(access_token, developer_token, customer_id, account_id):
    """SDK: GetAccountsInfo."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("AccountsInfo/Query"), {"CustomerId": customer_id}, "AccountsInfo"
    )


@dlt.resource(name="customer_info", write_disposition="merge", primary_key="Id")
def customer_info(access_token, developer_token, customer_id, account_id):
    """SDK: GetCustomer."""
    c = _client(access_token, developer_token, customer_id, account_id)
    data = post_rpc(c, _url("Customer/Query"), {"CustomerId": customer_id})
    customer = data.get("Customer")
    if customer:
        yield customer


@dlt.resource(
    name="customers_info", write_disposition="merge", primary_key="CustomerId"
)
def customers_info(access_token, developer_token, customer_id, account_id):
    """SDK: GetCustomersInfo."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(c, _url("CustomersInfo/Query"), {"TopN": 100}, "CustomersInfo")


@dlt.resource(name="current_user", write_disposition="replace")
def current_user(access_token, developer_token, customer_id, account_id):
    """SDK: GetCurrentUser."""
    c = _client(access_token, developer_token, customer_id, account_id)
    data = post_rpc(c, _url("User/QueryCurrent"), {})
    user = data.get("User")
    if user:
        yield user


@dlt.resource(name="users_info", write_disposition="merge", primary_key="Id")
def users_info(access_token, developer_token, customer_id, account_id):
    """SDK: GetUsersInfo."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c, _url("UsersInfo/Query"), {"CustomerId": customer_id}, "UsersInfo"
    )


@dlt.resource(name="customer_pilot_features", write_disposition="replace")
def customer_pilot_features(access_token, developer_token, customer_id, account_id):
    """SDK: GetCustomerPilotFeatures."""
    c = _client(access_token, developer_token, customer_id, account_id)
    data = post_rpc(c, _url("CustomerPilotFeatures/Query"), {"CustomerId": customer_id})
    features = data.get("FeaturePilotFlags", [])
    for f in features:
        yield {"customer_id": customer_id, "feature_id": f}


@dlt.resource(name="account_pilot_features", write_disposition="replace")
def account_pilot_features(access_token, developer_token, customer_id, account_id):
    """SDK: GetAccountPilotFeatures."""
    c = _client(access_token, developer_token, customer_id, account_id)
    data = post_rpc(c, _url("AccountPilotFeatures/Query"), {"AccountId": account_id})
    features = data.get("FeaturePilotFlags", [])
    for f in features:
        yield {"account_id": account_id, "feature_id": f}


@dlt.resource(name="linked_accounts_and_customers", write_disposition="replace")
def linked_accounts_and_customers(
    access_token, developer_token, customer_id, account_id
):
    """SDK: GetLinkedAccountsAndCustomersInfo."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(
        c,
        _url("LinkedAccountsAndCustomersInfo/Query"),
        {"CustomerId": customer_id},
        "AccountsInfo",
    )


@dlt.resource(name="notifications", write_disposition="append")
def notifications(access_token, developer_token, customer_id, account_id):
    """SDK: GetNotifications."""
    c = _client(access_token, developer_token, customer_id, account_id)
    yield from safe_rpc(c, _url("Notifications/Query"), {}, "Notifications")


ALL_CUSTOMER_MGMT_RESOURCES = [
    account_info,
    accounts_info,
    customer_info,
    customers_info,
    current_user,
    users_info,
    customer_pilot_features,
    account_pilot_features,
    linked_accounts_and_customers,
    notifications,
]
