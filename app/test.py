import json
import logging

import brownie
from brownie import Contract, accounts, network
from config import GRAPH_ENDPOINT, MULTICALL, ROUTER
from pipe import chain, dedup, select, where

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_router_contract(address):
    abi = None
    with open("./abis/Router.json") as f:
        abi = json.load(f)
    return Contract.from_abi("BufferRouter", address, abi)


def main():
    environment = "arb-goerli"
    brownie.multicall(address=MULTICALL[environment])
    print(MULTICALL[environment])
    router_contract = get_router_contract(ROUTER[environment])
    trades = []
    with brownie.multicall:
        queue_ids = list(range(3000, 3583))

        trades = [router_contract.queuedTrades(queue_id) for queue_id in queue_ids]
        print("leaving multicall")
    print(trades)


if __name__ == "__main__":
    network.connect("arb-goerli")
    main()
