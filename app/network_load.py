import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pprint import pprint

import brownie
import monitor
import requests
from brownie import Contract, accounts, network
from cache import disk_cache as cache
from config import MULTICALL

# from keeper_helper import resolve_queued_trades_v1, unlock_options_v1
from helper import resolve_queued_trades_v2, unlock_options_v2
from pipe import chain, dedup, select, where

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    rpcs = os.environ["RPC"].split(",")
    networks = os.environ["NETWORK"].split(",")
    for index, rpc in enumerate(rpcs):
        cmd = [
            "brownie",
            "networks",
            "add",
            os.environ["CHAIN_NAME"],
            networks[index],
            f"host={rpc}",
            f"chainid={os.environ['CHAIN_ID']}",
            f"explorer={os.environ['EXPLORER']}",
        ]
        subprocess.run(cmd)
