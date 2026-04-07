"""Shared utilities for dlt-community-sources."""

import logging

from dlt.sources import DltResource

logger = logging.getLogger(__name__)


def wrap_resources_safe(resources: list[DltResource]) -> list[DltResource]:
    """Wrap each resource's generator to catch and log errors.

    When one resource fails during extraction, the error is logged and
    the resource yields nothing instead of crashing the entire pipeline.
    """
    for r in resources:
        gen = r._pipe.gen
        if callable(gen):
            resource_name = r.name

            def _make_wrapper(gen_fn, name):
                def wrapper(*args, **kwargs):
                    try:
                        yield from gen_fn(*args, **kwargs)
                    except Exception as e:
                        logger.warning("Resource %s failed, skipping: %s", name, e)

                return wrapper

            r._pipe.replace_gen(_make_wrapper(gen, resource_name))
    return resources
