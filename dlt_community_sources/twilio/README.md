# Twilio

A dlt source for [Twilio API](https://www.twilio.com/docs/api).

## Installation

```bash
pip install dlt-community-sources[twilio]
```

## Usage

```python
import dlt
from dlt_community_sources.twilio import twilio_source

pipeline = dlt.pipeline(
    pipeline_name="twilio",
    destination="bigquery",
    dataset_name="source_twilio",
)

source = twilio_source(
    account_sid="YOUR_ACCOUNT_SID",
    api_key_sid="YOUR_API_KEY_SID",
    api_key_secret="YOUR_API_KEY_SECRET",
)

load_info = pipeline.run(source)
```

### Load specific resources

```python
source = twilio_source(
    account_sid="YOUR_ACCOUNT_SID",
    api_key_sid="YOUR_API_KEY_SID",
    api_key_secret="YOUR_API_KEY_SECRET",
    resources=["messages", "calls", "usage_records"],
)
```

## Resources

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `messages` | append | by date_sent | SMS/MMS messages |
| `calls` | append | by start_time | Voice calls |
| `accounts` | merge | - | Accounts and subaccounts |
| `usage_records` | merge | by start_date | Daily usage records |
| `recordings` | append | by date_created | Call recordings |
| `transcriptions` | merge | - | Transcriptions of recordings |
| `conferences` | append | by date_created | Conference calls |
| `queues` | merge | - | Call queues |
| `incoming_phone_numbers` | merge | - | Phone numbers owned by the account |
| `available_phone_numbers` | replace | - | Available phone numbers for purchase |
| `addresses` | merge | - | Addresses on the account |
| `keys` | merge | - | API keys |
| `outgoing_caller_ids` | merge | - | Verified outgoing caller IDs |
| `applications` | merge | - | TwiML applications |
| `connect_apps` | merge | - | Connect apps |
| `notifications` | append | by message_date | Log notifications |
| `sip_domains` | merge | - | SIP domains |
| `sip_ip_access_control_lists` | merge | - | SIP IP access control lists |
| `sip_credential_lists` | merge | - | SIP credential lists |

## Authentication

Two authentication methods are supported.

### Auth Token (simple)

```python
source = twilio_source(
    account_sid="YOUR_ACCOUNT_SID",
    auth_token="YOUR_AUTH_TOKEN",
)
```

| Parameter | Description |
|---|---|
| `account_sid` | Your Twilio Account SID (starts with `AC`) |
| `auth_token` | Your Twilio Auth Token |

### API Key (recommended for production)

API keys can be scoped to specific permissions and revoked independently.

```python
source = twilio_source(
    account_sid="YOUR_ACCOUNT_SID",
    api_key_sid="YOUR_API_KEY_SID",
    api_key_secret="YOUR_API_KEY_SECRET",
)
```

| Parameter | Description |
|---|---|
| `account_sid` | Your Twilio Account SID (starts with `AC`) |
| `api_key_sid` | API Key SID (starts with `SK`) |
| `api_key_secret` | API Key Secret |

Create API keys at [Twilio Console > API keys & tokens](https://console.twilio.com/us1/account/keys-credentials/api-keys).

## Notes

- **Permission errors**: Resources that return 403 or 404 are silently skipped. The pipeline continues with accessible resources.
- **API Key (Standard)**: Cannot access `accounts` resource. Use Auth Token if you need account data.
- **Free trial accounts**: Some resources may return 403 depending on your account type.
- **available_phone_numbers**: Defaults to US (`country_code="US"`). Pass a different country code to the resource if needed.
- **Credentials required**: Either `auth_token` or both `api_key_sid` and `api_key_secret` must be provided. Omitting both raises `ValueError`.
