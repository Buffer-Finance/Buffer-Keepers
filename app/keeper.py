import argparse
import json
import logging
import multiprocessing as mp
import os
import threading
import time
from multiprocessing import Process
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
environment = os.environ["ENVIRONMENT"]


def create_process(target, name):
    process = Process(name=name, target=target, args=(environment,))
    # start the new process
    process.start()
    # wait for the new process to finish
    process.join()


def excepthook(args):
    current_process = mp.current_process().name
    logger.info(f"Execption occured in {current_process}")
    counter = int(current_process.split("-")[1]) + 1

    # # create a new process with the same config
    if "Open" in current_process:
        logger.info(f"Restarting open bot...")
        create_process(open_v2, f"OpenTask-{counter}")
    elif "Close" in current_process:
        logger.info(f"Restarting close bot...")
        create_process(close_v2, f"CloseTask-{counter}")


threading.excepthook = excepthook


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

    if args.bot == "open":
        create_process(open_v2, "OpenTask-1")

    elif args.bot == "close":
        create_process(close_v2, "CloseTask-1")
