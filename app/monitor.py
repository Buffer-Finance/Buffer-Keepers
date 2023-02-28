# pylint: disable=E0611,E0401

import os

import sentry_sdk

sentry_sdk.init(
    dsn=os.environ["SENTRY_DSN"],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production,
    # traces_sample_rate=1.0,
)
