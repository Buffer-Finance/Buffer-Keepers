# pylint: disable=E0611,E0401

import logging
import os

import brownie
import sentry_sdk

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
sentry_sdk.init(
    dsn=os.environ["WALLET_SENTRY_DSN"],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production,
    # traces_sample_rate=1.0,
)


def check_wallet(account):

    balance = account.balance() / 1e18
    if balance < float(os.environ["MIN_BALANCE"]):
        sentry_sdk.capture_message(
            f"{account} is low on funds on {os.environ['ENVIRONMENT']} chain. Balance: {round(balance, 2)} ETH",
            f"{account}",
        )
