import argparse
import json
import logging
import os
import time
from pprint import pprint

import brownie
import monitor
import requests
from brownie import Contract, accounts, network
from config import MULTICALL

# from keeper_helper import resolve_queued_trades_v1, unlock_options_v1
from helper import resolve_queued_trades_v2, unlock_options_v2
from pipe import chain, dedup, select, where

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


parser = argparse.ArgumentParser(description="Keeper Bots")

parser.add_argument(
    "--bot",
    type=str,
)


def open_v2(environment):
    while True:
        try:
            resolve_queued_trades_v2(environment)
        except Exception as e:
            if "429" in str(e):
                logger.info(f"Handled rpc error {e}")
            elif "unsupported block number" in str(e):
                logger.info(f"Handled rpc error {e}")
            else:
                logger.exception(e)
            time.sleep(int(os.environ["WAIT_TIME"]))
        time.sleep(int(os.environ["DELAY"]))


def close_v2(environment):
    while True:
        # logger.info("ping")
        try:
            unlock_options_v2(environment)
        except Exception as e:
            if "429" in str(e):
                logger.info("Handled rpc error")
            elif "unsupported block number" in str(e):
                logger.info(f"Handled rpc error {e}")
            else:
                logger.exception(e)
            time.sleep(int(os.environ["WAIT_TIME"]))
        time.sleep(int(os.environ["DELAY"]))


if __name__ == "__main__":
    args = parser.parse_args()

    network.connect(os.environ["NETWORK"])
    logger.info(f"connected {network.show_active()}")
    # brownie.multicall(address=MULTICALL[chain])

    environment = os.environ["ENVIRONMENT"]

    if args.bot == "open":
        open_v2(environment)
    elif args.bot == "close":
        close_v2(environment)
