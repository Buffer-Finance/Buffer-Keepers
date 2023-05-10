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
from monitor_wallet import check_wallet
from pipe import chain, dedup, select, sort, where

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# import grequests

open_keeper_account = accounts.add(os.environ["OPEN_KEEPER_ACCOUNT_PK"])
close_keeper_account = accounts.add(os.environ["CLOSE_KEEPER_ACCOUNT_PK"])

MAX_BATCH_SIZE = 100


def get_queue_ids_from_graph(json_data, endpoint):

    response = requests.post(
        endpoint,
        json=json_data,
    )

    response.raise_for_status()
    return response.json()["data"]


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


def get_keeper_reader_contract(address):
    abi = None
    with open("./abis/KeeperReader.json") as f:
        abi = json.load(f)
    return Contract.from_abi("KeeperReader", address, abi)


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


def _unlock_options(expired_options, environment):
    if not expired_options:
        return

    logger.debug(f"expired_options from theGraph: {_(expired_options)}")

    # Filter out the ones for invalid pairs
    target_option_contracts_mapping = list(
        expired_options
        | select(lambda x: x["contractAddress"])
        | dedup
        | select(
            lambda options_contract: (
                options_contract,
                get_asset_pair(options_contract, environment),
            )
        )
    )

    target_option_contracts_mapping = dict(
        target_option_contracts_mapping
        | select(lambda x: (x[0], x[1].replace("-", "")))
    )

    # Take the initial 100
    expired_options = expired_options[:MAX_BATCH_SIZE]

    brownie.multicall(address=config.MULTICALL[environment])

    with brownie.multicall:

        # Confirm if these options are still active by using RPC calls
        expired_options = list(
            expired_options
            | select(
                lambda x: (
                    x,
                    get_options_contract(x["contractAddress"]).options(x["optionID"]),
                )
            )
        )

        expired_options = list(
            expired_options | where(lambda x: x[1][0] == 1) | select(lambda x: x[0])
        )

    if not expired_options:
        return

    logger.info(f"expired_options: {_(expired_options)}")
    # Fetch the prices for the valid ones
    prices_to_fetch = list(
        expired_options
        | select(
            lambda x: f'{target_option_contracts_mapping[x["contractAddress"]]}%{x["expirationTime"]}',
        )
        | dedup
        | select(lambda x: x.split("%"))
        | select(
            lambda x: {
                "pair": x[0],
                "timestamp": int(x[1]),
            }
        )
    )  # List[(assetPair, timestamp)]

    logger.debug(f"prices_to_fetch: {_(prices_to_fetch)}")
    fetched_prices_mapping = fetch_prices(prices_to_fetch)

    _price = lambda x: fetched_prices_mapping.get(
        f"{target_option_contracts_mapping[x['contractAddress']]}-{x['expirationTime']}",
        {},
    )

    unlock_payload = list(
        expired_options
        | where(lambda x: _price(x).get("price"))
        | select(
            lambda x: (
                x["optionID"],
                x["contractAddress"],
                x["expirationTime"],
                _price(x)["price"],  # price
                _price(x)["signature"],  # signature
            )
        )
        | dedup(key=lambda x: f"{x[0]}-{x[1]}")
    )

    if unlock_payload:
        logger.info(f"unlock_payload: {_(unlock_payload)}")
        router = get_router_contract(config.ROUTER[environment])
        params = {
            "from": close_keeper_account,
            "gas": config.GAS_PRICE[environment],
            "required_confs": int(os.environ["CONFS"]),
            "max_fee": (2 * brownie.chain.base_fee) + brownie.chain.priority_fee,
            "priority_fee": brownie.chain.priority_fee,
            "allow_revert": True,
        }
        gas = router.unlockOptions.estimate_gas(unlock_payload, params) * 1.01

        logger.info(f"Transacting at {gas} gas units...")
        try:
            router.unlockOptions(
                unlock_payload,
                {**params, "gas_limit": gas},
            )
            check_wallet(close_keeper_account)
        except Exception as e:
            if "nonce too low" in str(e):
                logger.info(e)
            else:
                logger.exception(e)


def _(x):
    return json.dumps(x, indent=4, sort_keys=True)


def _resolve_queued_trades(_queue_ids, environment):
    queue_ids = list(
        _queue_ids
        | where(lambda x: x["state"] == 4)
        | select(lambda x: int(x["queueID"]))
        | dedup
        | sort(key=lambda x: x)
    )

    if queue_ids:
        logger.debug(f"Queue ids from theGraph: {_(queue_ids)}")

    unresolved_trades = []
    keeper_reader = get_keeper_reader_contract(config.KEEPER_READER[environment])
    logger.debug(f"keeper_reader: {keeper_reader}")

    queue_ids = queue_ids[:MAX_BATCH_SIZE]
    # TODO: Need to handle the case where the queue is too long
    unresolved_trades = keeper_reader.retrieveTrades(queue_ids, MAX_BATCH_SIZE)

    logger.debug(f"{len(unresolved_trades)} unresolved_trades")

    if not unresolved_trades:
        return

    logger.info(f"unresolved_trades: {_(unresolved_trades)}")

    target_option_contracts_mapping = dict(
        unresolved_trades
        | select(lambda x: x[1])
        | dedup
        | select(
            lambda options_contract: (
                options_contract,
                get_asset_pair(options_contract, environment).replace("-", ""),
            )
        )
    )

    prices_to_fetch = list(
        unresolved_trades
        | select(
            lambda x: f"{target_option_contracts_mapping[x[1]]}%{x[2]}",
        )
        | dedup
        | select(lambda x: x.split("%"))
        | select(
            lambda x: {
                "pair": x[0],
                "timestamp": int(x[1]),
            }
        )
    )  # List[(assetPair, timestamp)]

    logger.debug(f"prices_to_fetch: {_(prices_to_fetch)}")
    fetched_prices_mapping = fetch_prices(prices_to_fetch)
    logger.debug(f"fetched_prices_mapping: {_(fetched_prices_mapping)}")

    _price = lambda x: fetched_prices_mapping.get(
        f"{target_option_contracts_mapping[x[1]]}-{x[2]}",
        {},
    )

    unresolved_trades = list(
        unresolved_trades
        | where(lambda x: _price(x).get("price"))
        | select(
            lambda x: (
                x[0],  # queueId
                x[2],  # timestamp
                _price(x)["price"],  # price
                _price(x)["signature"],  # signature
            )
        )
        | dedup(key=lambda x: x[0])
    )  # List[(queueId, timestamp, price, signature)]

    if unresolved_trades:
        logger.info(f"resolve payload: {_(unresolved_trades)}")
        router_contract = get_router_contract(config.ROUTER[environment])
        params = {
            "from": open_keeper_account,
            "gas": config.GAS_PRICE[environment],
            "required_confs": int(os.environ["CONFS"]),
            "allow_revert": True,
            "max_fee": (2 * brownie.chain.base_fee) + brownie.chain.priority_fee,
            "priority_fee": brownie.chain.priority_fee,
        }
        gas = (
            router_contract.resolveQueuedTrades.estimate_gas(unresolved_trades, params)
            * 1.01
        )
        logger.info(f"Transacting at {gas}  gas units...")
        try:
            router_contract.resolveQueuedTrades(
                unresolved_trades, {**params, "gas_limit": gas}
            )
            check_wallet(open_keeper_account)
        except Exception as e:
            if "nonce too low" in str(e):
                logger.info(e)
            else:
                logger.exception(e)


def resolve_queued_trades_v2(environment):
    logger.info(f"{mp.current_process().name} {datetime.now()}")
    json_data = {
        "query": "query MyQuery {\n  queuedOptionDatas(\n    orderBy: queueID\n    orderDirection: desc\n    where: {state_in: [4, 5, 6]}\n  first: 1000\n) {\n    queueID\n  state\n}\n}",
        "variables": None,
        "operationName": "MyQuery",
        "extensions": {
            "headers": None,
        },
    }
    queuedOptionDatas = []
    try:
        response = get_queue_ids_from_graph(
            json_data,
            config.GRAPH_ENDPOINT[environment],
        )
        queuedOptionDatas = response["queuedOptionDatas"]
    except Exception as e:
        logger.info(f"Error fetching from theGraph")

    _resolve_queued_trades(
        queuedOptionDatas,
        environment=environment,
    )


def unlock_options_v2(environment):
    logger.info(f"{mp.current_process().name} {datetime.now()}")
    limit = 1000

    json_data = {
        "query": "query UserOptionHistory($currentTimestamp: BigInt = "
        + str(int(time.time()))
        + ") {\n  userOptionDatas(\n    orderBy: creationTime\n    orderDirection: asc\n    where: {state_in: [1], expirationTime_lt: $currentTimestamp}\n    first: "
        + str(limit)
        + " ) {\n    optionID\n    optionContract {address} \n  expirationTime\n }\n}",
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
    _unlock_options(
        expired_options,
        environment=environment,
    )
