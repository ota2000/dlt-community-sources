"""Shared utilities for dlt-community-sources."""

import logging

from dlt.sources import DltResource
from dlt.sources.helpers.requests import HTTPError

logger = logging.getLogger(__name__)

# HTTP status codes that indicate "no data" (not an error worth retrying)
_SKIP_STATUS_CODES = {400, 403, 404}


def wrap_resources_safe(resources: list[DltResource]) -> list[DltResource]:
    """Wrap each resource's generator to catch and log expected errors.

    Only skips resources that fail with HTTP 400/403/404 (no data, no
    permission, not found). All other errors (429 after retries, 5xx,
    connection errors) are raised to stop the pipeline, since they
    indicate a real problem that should be investigated.
    """
    for r in resources:
        gen = r._pipe.gen
        if callable(gen):
            resource_name = r.name

            def _make_wrapper(gen_fn, name):
                def wrapper(*args, **kwargs):
                    try:
                        yield from gen_fn(*args, **kwargs)
                    except HTTPError as e:
                        if (
                            e.response is not None
                            and e.response.status_code in _SKIP_STATUS_CODES
                        ):
                            logger.info(
                                "Resource %s skipped: %d %s",
                                name,
                                e.response.status_code,
                                e.response.reason,
                            )
                        else:
                            raise
                    except Exception:
                        raise

                return wrapper

            r._pipe.replace_gen(_make_wrapper(gen, resource_name))
    return resources
