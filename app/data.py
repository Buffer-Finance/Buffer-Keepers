import json
import logging
import multiprocessing as mp
import os
import time
from datetime import datetime

import brownie
import config
import requests
from brownie import Contract, accounts, network
from cache import disk_cache as cache
from eth_account import Account
from eth_account.messages import encode_defunct
from monitor_wallet import check_wallet
from pipe import chain, dedup, select, sort, where

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# import grequests

open_keeper_account = accounts.add(os.environ["OPEN_KEEPER_ACCOUNT_PK"])
close_keeper_account = accounts.add(os.environ["CLOSE_KEEPER_ACCOUNT_PK"])

MAX_BATCH_SIZE = 100


def get_option_ids_to_unlock_from_graph(json_data, endpoint):

    response = requests.post(
        endpoint,
        json=json_data,
    )

    response.raise_for_status()

    return list(
        response.json()["data"]["userOptionDatas"]
    )  # List[{optionId, contractAddress, expirationTime}]


def get_router_contract(address):
    abi = None
    with open("./abis/Router.json") as f:
        abi = json.load(f)
    return Contract.from_abi("BufferRouter", address, abi)


def get_options_contract(address):
    abi = None
    with open(f"./abis/BufferOptions.json") as f:
        abi = json.load(f)
    return Contract.from_abi("BufferOptions", address, abi)


def fetch_prices(prices_to_fetch):
    query_key = lambda x: f"{x['pair']}-{x['timestamp']}"

    cached_response = {}
    uncached_prices_to_fetch = []
    for x in prices_to_fetch:
        val = cache.get(query_key(x))
        if val:
            cached_response[query_key(x)] = val
        else:
            uncached_prices_to_fetch.append(x)

    reqUrl = "https://oracle.buffer-finance-api.link/price/query/"

    fetched_prices = []
    if uncached_prices_to_fetch:

        def f(uncached_prices_to_fetch):
            r = requests.post(reqUrl, json=uncached_prices_to_fetch)

            try:
                r.raise_for_status()
            except Exception as e:
                logger.info(r.text)
                raise e

            fetched_prices = r.json()
            # fetch_prices = grequests.map(
            #     (
            #         grequests.post(reqUrl, json=_prices_to_fetch)
            #         for _prices_to_fetch in list(prices_to_fetch | batch(5))
            #     )
            # )
            # logger.info(fetch_prices)
            return dict(
                fetched_prices
                | where(lambda x: x["signature"] is not None)
                | select(
                    lambda x: (
                        query_key(x),
                        {"price": x["price"], "signature": x["signature"]},
                    )
                )
            )

        response = f(uncached_prices_to_fetch)
        NUM_RETRY = 10
        while not response and NUM_RETRY > 0:
            time.sleep(0.3)
            response = f(uncached_prices_to_fetch)
            NUM_RETRY -= 1

        if response:
            now = time.time()
            try:
                logger.info(
                    f"#### Price Fetching Lags: {list(list(response.keys()) | select(lambda x: (x, round(now - int(x.split('-')[1]), 2))))}"
                )
            except Exception as e:
                logger.info(e)

            # Cache the response so that we don't have to fetch it again
            for k, v in response.items():
                cache.set(k, v)

        response.update(cached_response)
        return response
        # asset_pair-timestamp ==> price

    return cached_response


def get_asset_pair(option_contract_address, environment):
    r = cache.get(f"{option_contract_address}-{environment}-asset_pair")
    if r:
        return r
    r = get_options_contract(option_contract_address).assetPair()
    cache.set(f"{option_contract_address}-{environment}-asset_pair", r)
    return r


def get_market_info(environment, order_by="-queued_timestamp"):
    reqUrl = f"{config.BASE_URL}/trades/all_active/?environment={brownie.network.chain.id}&user_signature={keeper_signature()}&order_by={order_by}"
    r = requests.get(reqUrl)
    # logger.info(f"get_market_info: {reqUrl}")
    response = {}
    for market in r.json():
        response.update(
            {
                market["queue_id"]: {
                    "queue_id": market["queue_id"],
                    "queued_timestamp": market["queued_timestamp"],
                    "state": market["state"],
                    "is_above": market["is_above"],
                    "sign_info": (
                        market["user_full_signature"],
                        market["signature_timestamp"],
                    ),
                    "expiration_time": market["expiration_time"],
                },
            },
        )
    # logger.info(f"get_market_info: {response}")

    return response


def get_limit_orders(environment):
    reqUrl = f"{config.BASE_URL}/trades/all_limit_orders/?environment={brownie.network.chain.id}&user_signature={keeper_signature()}"
    r = requests.get(reqUrl)
    logger.info(f"get_market_info: {reqUrl} : {r.json()}")
    return r.json()


def get_queued_trades(environment):
    reqUrl = f"{config.BASE_URL}/trades/all_pending/?environment={brownie.network.chain.id}&user_signature={keeper_signature()}"
    r = requests.get(reqUrl)
    logger.info(f"all_pending: {reqUrl} : {r.json()}")
    return r.json()


def get_sf(environment):
    reqUrl = f"{config.BASE_URL}/settlement_fee/?environment={brownie.network.chain.id}"
    r = requests.get(reqUrl)
    # logger.info(f"get_market_info: {reqUrl} : {r.json()}")
    return r.json()


def get_expired_options(environment):
    limit = 1000
    db_data = get_market_info(environment, order_by="expiration_time")
    # db_data = dict(
    #     db_data
    #     # | where(lambda x: (db_data[x]["expiration_time"] <= int(time.time(), )))
    #     | select(lambda x: (x, db_data[x]))
    # )

    if not db_data:
        return [], []
    logger.info(f"db_data: {db_data}")

    min_timestamp = min([db_data[x]["queued_timestamp"] for x in db_data])
    json_data = {
        "query": "query UserOptionHistgry($minTimestamp: BigInt = "
        + str(min_timestamp)
        + ") {\n  userOptionDatas(\n    orderBy: creationTime\n    orderDirection: asc\n    where: {state_in: [2,3], expirationTime_gte: $minTimestamp}\n    first: "
        + str(limit)
        + " ) {\n    optionID\n queueID\n payout\n  expirationPrice\n closeTime\n  optionContract {address} \n  expirationTime\n }\n}",
        "variables": None,
        "operationName": "UserOptionHistory",
        "extensions": {
            "headers": None,
        },
    }
    # Fetch from theGraph
    graph_data = []
    try:
        graph_data = get_option_ids_to_unlock_from_graph(
            json_data, config.GRAPH_ENDPOINT[environment]
        )  # List[{optionID, contractAddress, expirationTime}]
    except Exception as e:
        logger.info(f"Error fetching from theGraph {e}")
        time.sleep(5)

    graph_data = list(
        graph_data
        | select(
            lambda x: {
                **x,
                "contractAddress": x["optionContract"]["address"],
            }
        )
    )
    logger.info(f"graph_data: {graph_data}")
    return graph_data, db_data


def get_option_to_expire(environment):
    limit = 1000

    json_data = {
        "query": "query UserOptionHistory($currentTimestamp: BigInt = "
        + str(int(time.time()))
        + ") {\n  userOptionDatas(\n    orderBy: creationTime\n    orderDirection: asc\n    where: {state_in: [1], expirationTime_lt: $currentTimestamp}\n    first: "
        + str(limit)
        + " ) {\n    optionID\n queueID\n    optionContract {address} \n  expirationTime\n }\n}",
        "variables": None,
        "operationName": "UserOptionHistory",
        "extensions": {
            "headers": None,
        },
    }
    # Fetch from theGraph
    expired_options = []
    try:
        expired_options = get_option_ids_to_unlock_from_graph(
            json_data, config.GRAPH_ENDPOINT[environment]
        )  # List[{optionID, contractAddress, expirationTime}]
    except Exception as e:
        logger.info(f"Error fetching from theGraph")
        time.sleep(5)

    expired_options = list(
        expired_options
        | select(
            lambda x: {
                **x,
                "contractAddress": x["optionContract"]["address"],
            }
        )
    )
    logger.info(f"expired_options: {expired_options}")
    return expired_options


def keeper_signature():
    web3 = brownie.network.web3
    key = open_keeper_account.private_key
    message = encode_defunct(text="Sign to verify user address")
    signed_message = Account.sign_message(message, key)

    def to_32byte_hex(val):
        return web3.toHex(web3.toBytes(val).rjust(32, b"\0"))

    return to_32byte_hex(signed_message.signature)
