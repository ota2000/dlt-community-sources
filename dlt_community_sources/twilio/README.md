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
    auth_token="YOUR_AUTH_TOKEN",
)

load_info = pipeline.run(source)
```

### Load specific resources

```python
source = twilio_source(
    account_sid="YOUR_ACCOUNT_SID",
    auth_token="YOUR_AUTH_TOKEN",
    resources=["messages", "calls", "usage_records"],
)
```

## Resources

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `messages` | append | by date_sent | SMS/MMS messages |
| `calls` | append | by start_time | Voice calls |
| `accounts` | merge | - | Accounts and subaccounts |
| `usage_records` | append | by start_date | Daily usage records |
| `recordings` | append | by date_created | Call recordings |
| `transcriptions` | append | - | Transcriptions of recordings |
| `conferences` | append | by date_created | Conference calls |
| `queues` | merge | - | Call queues |
| `incoming_phone_numbers` | merge | - | Phone numbers owned by the account |
| `available_phone_numbers` | replace | - | Available phone numbers for purchase |
| `addresses` | merge | - | Addresses on the account |
| `keys` | merge | - | API keys |

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
