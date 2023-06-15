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


def get_opened_option_ids(min_timestamp, environment):
    endpoint = config.GRAPH_ENDPOINT[environment]
    limit = 1000
    json_data = {
        "query": "query UserOptionHistory($minTimestamp: BigInt = "
        + str(int(min_timestamp))
        + ") {\n  userOptionDatas(\n    orderBy: creationTime\n    orderDirection: asc\n    where: {state_in: [1], creationTime_gt: $minTimestamp}\n    first: "
        + str(limit)
        + " ) {\n    optionID\n    optionContract {address} \n  queueId\n }\n}",
        "variables": None,
        "operationName": "UserOptionHistory",
        "extensions": {
            "headers": None,
        },
    }
    response = requests.post(
        endpoint,
        json=json_data,
    )

    response.raise_for_status()

    return list(
        response.json()["data"]["userOptionDatas"]
    )  # List[{optionId, contractAddress, queueId}]


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


def get_market_directions(market_data_to_fetch):
    # Call api to get this data
    # list({
    #     (optionID-contractAddress) : {
    #         "queue_id": 0,
    #         "is_above": True,
    #         'sign_info' : list(full_signature, signature_timestamp)
    #     }
    # })

    return []


def _(x):
    return json.dumps(x, indent=4, sort_keys=True)


def _unlock_options(expired_options, environment):

    if not expired_options:
        return

    logger.info(f"expired_options from theGraph: {_(expired_options)}")

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
    print(target_option_contracts_mapping)

    target_option_contracts_mapping = dict(
        target_option_contracts_mapping
        | select(lambda x: (x[0], x[1].replace("-", "")))
    )

    # Take the initial 100
    expired_options = expired_options[:MAX_BATCH_SIZE]
    router = get_router_contract(config.ROUTER[environment])
    options_contracts = {
        x["contractAddress"]: get_options_contract(x["contractAddress"])
        for x in expired_options
    }
    logger.info(f"options_contracts: {(options_contracts)}")
    brownie.multicall(address=config.MULTICALL[environment])

    with brownie.multicall:
        # Confirm if these options are still active by using RPC calls
        logger.info("here")
        option_details = [
            options_contracts[x["contractAddress"]].options(x["optionID"])
            for x in expired_options
        ]
        queue_ids = [
            router.optionIdMapping(x["contractAddress"], x["optionID"])
            for x in expired_options
        ]
    expired_options = list(zip(expired_options, option_details, queue_ids))

    expired_options = list(
        expired_options
        | where(lambda x: x[1][0] == 1)
        | select(
            lambda x: {
                "contractAddress": x[0]["contractAddress"],
                "optionID": x[0]["optionID"],
                "expirationTime": x[0]["expirationTime"],
                "queueId": x[2],
                # "queueId": 2,
            }
        )
    )
    logger.info(f"expired_options: {expired_options}")

    if not expired_options:
        return

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
    market_info = get_market_info(environment)

    logger.info(f"market_data_to_fetch: {(market_info)}")
    logger.info(f"prices_to_fetch: {_(prices_to_fetch)}")
    fetched_prices_mapping = fetch_prices(prices_to_fetch)

    _price = lambda x: fetched_prices_mapping.get(
        f"{target_option_contracts_mapping[x['contractAddress']]}-{x['expirationTime']}",
        {},
    )
    logger.info(f"expired_options: {expired_options}")

    unlock_payload = list(
        expired_options
        | where(lambda x: _price(x).get("price"))
        | select(
            lambda x: (
                x["queueId"],
                x["optionID"],
                x["contractAddress"],
                _price(x)["price"],  # price
                market_info[x["queueId"]]["is_above"],  # isAbove
                market_info[x["queueId"]][
                    "sign_info"
                ],  # list([full_signature, signature_timestamp])
                [_price(x)["signature"], x["expirationTime"]],  # signature
            )
        )
        | dedup(key=lambda x: f"{x[0]}-{x[1]}")
    )

    if unlock_payload:
        logger.info(f"unlock_payload: {(unlock_payload)}")
        # params = {
        #     "from": close_keeper_account,
        #     "gas": config.GAS_PRICE[environment],
        #     "required_confs": int(os.environ["CONFS"]),
        #     "max_fee": (2 * brownie.chain.base_fee) + brownie.chain.priority_fee,
        #     "priority_fee": brownie.chain.priority_fee,
        #     "allow_revert": True,
        # }
        # gas = router.executeOptions.estimate_gas(unlock_payload, params) * 1.01

        # logger.info(f"Transacting at {gas} gas units...")
        # try:
        #     router.executeOptions(
        #         unlock_payload,
        #         {**params, "gas_limit": gas},
        #     )
        #     check_wallet(close_keeper_account)
        # except Exception as e:
        #     if "nonce too low" in str(e):
        #         logger.info(e)
        #     else:
        #         logger.exception(e)


# class UnlockState(BaseModel):
#     queue_id: int
#     payout: int
#     close_time: int
#     expiry_price: int


def _resolve_queued_trades(queued_trades, environment):
    if not queued_trades:
        return

    logger.info(f"queued_trades: {_(queued_trades)}")

    target_option_contracts_mapping = dict(
        queued_trades
        | select(lambda x: x["target_contract"])
        | dedup
        | select(
            lambda options_contract: (
                options_contract,
                get_asset_pair(options_contract, environment).replace("-", ""),
            )
        )
    )
    logger.info(
        f"target_option_contracts_mapping: {_(target_option_contracts_mapping)}"
    )

    prices_to_fetch = list(
        queued_trades
        | select(
            lambda x: {
                "pair": target_option_contracts_mapping[x["target_contract"]],
                "timestamp": int(x.get("queued_timestamp", 0)),
            }
        )
    )  # List[(assetPair, timestamp)]

    logger.info(f"prices_to_fetch: {_(prices_to_fetch)}")
    fetched_prices_mapping = fetch_prices(prices_to_fetch)
    logger.info(f"fetched_prices_mapping: {_(fetched_prices_mapping)}")

    _price = lambda x: fetched_prices_mapping.get(
        f"{target_option_contracts_mapping[x['target_contract']]}-{x['queued_timestamp']}",
        {},
    )

    queued_trades = list(
        queued_trades
        | where(lambda x: _price(x).get("price"))
        | select(
            lambda x: (
                x["queue_id"],  # queueId
                x["user_address"],  # userAddress,
                x["trade_size"],  # price,
                x["period"],  # signature,
                x["target_contract"],  # signature,
                x["strike"],
                x["slippage"],
                x["allow_partial_fill"],
                x["referral_code"],
                x["trader_nft_id"],
                _price(x)["price"],  # price,
                x["settlement_fee"],
                x["is_limit_order"],
                x["limit_order_expiration"],
                [
                    x["settlement_fee_signature"],
                    x["settlement_fee_sign_expiration"],
                ],  # signature,
                [x["user_partial_signature"], x["signature_timestamp"]],
                [_price(x)["signature"], x.get("queued_timestamp", 0)],  # signature
            )
        )
        | dedup(key=lambda x: x[0])
    )  # List[(queueId, timestamp, price, signature)]

    if queued_trades:
        logger.info(f"resolve payload: {_(queued_trades)}")
        router_contract = get_router_contract(config.ROUTER[environment])
        params = {
            "from": open_keeper_account,
            "gas": config.GAS_PRICE[environment],
            "required_confs": int(os.environ["CONFS"]),
            "allow_revert": True,
            "max_fee": (2 * brownie.chain.base_fee) + brownie.chain.priority_fee,
            "priority_fee": brownie.chain.priority_fee,
        }

        gas = router_contract.openTrades.estimate_gas(queued_trades, params) * 1.01
        logger.info(f"Transacting at {gas}  gas units...")
        try:
            router_contract.openTrades(queued_trades, {**params, "gas_limit": gas})
            check_wallet(open_keeper_account)
            # TODO : Update the state here
        except Exception as e:
            if "nonce too low" in str(e):
                logger.info(e)
            else:
                logger.exception(e)


def get_all_trades():
    # print(open_keeper_account.address, brownie.network.chain.id)
    reqUrl = f"https://oracle.buffer-finance-api.link/instant-trading/trades/all/?user_signature={keeper_signature()}&user_address={open_keeper_account.address}&environment={brownie.network.chain.id}"
    logger.info(f"resolve_queued_trades_v2: {reqUrl}")
    r = requests.get(reqUrl)
    # print(reqUrl, r.json())
    print(r.json())

    return r.json()


def get_market_info(environment, order_by="-queued_timestamp"):
    reqUrl = f"https://oracle.buffer-finance-api.link/instant-trading/trades/all_active/?environment={brownie.network.chain.id}&user_signature={keeper_signature()}&order_by={order_by}"
    r = requests.get(reqUrl)
    logger.info(f"get_market_info: {reqUrl}")
    response = {}
    for market in r.json():
        response.update(
            {
                market["queue_id"]: {
                    "queue_id": market["queue_id"],
                    "is_above": market["is_above"],
                    "sign_info": (
                        market["user_full_signature"],
                        market["signature_timestamp"],
                    ),
                    "expiration_time": market["expiration_time"],
                },
            },
        )
    logger.info(f"get_market_info: {response}")

    return response


def get_sf(asset_pair):
    reqUrl = f"https://oracle.buffer-finance-api.link/instant-trading/settlement_fee/?asset_pair={asset_pair}&environment={brownie.network.chain.id}"
    r = requests.get(reqUrl)
    print(r.json())
    return r.json()["USDC"]


def is_strike_valid(strike_slippage, price):
    return True


def unlock_options_v2(environment):
    logger.info(f"{mp.current_process().name} {datetime.now()}")
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
    _unlock_options(
        expired_options,
        environment=environment,
    )


def update_db_after_unlock(environment):
    logger.info(f"{mp.current_process().name} {datetime.now()}")
    limit = 1000

    trades = get_market_info(environment, order_by="expiration_time")
    logger.info(f"trades: {trades}")

    min_timestamp = min([trades[x]["expiration_time"] for x in trades])
    logger.info(f"min_timestamp: {min_timestamp}")
    json_data = {
        "query": "query UserOptionHistgry($minTimestamp: BigInt = "
        + str(min_timestamp)
        + ") {\n  userOptionDatas(\n    orderBy: creationTime\n    orderDirection: asc\n    where: {state_in: [2,3], expirationTime_gte: $minTimestamp}\n    first: "
        + str(limit)
        + " ) {\n    optionID\n queueID\n payout\n  expirationPrice\n  optionContract {address} \n  expirationTime\n }\n}",
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
        logger.info(f"Error fetching from theGraph {e}")
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
    router = get_router_contract(config.ROUTER[environment])

    brownie.multicall(address=config.MULTICALL[environment])
    with brownie.multicall:
        # Confirm if these options are still active by using RPC calls
        logger.info("here")

        queue_ids = [
            router.optionIdMapping(x["contractAddress"], x["optionID"])
            for x in expired_options
        ]

    payload = list(
        zip(expired_options, queue_ids)
        | where(lambda x: x[0]["payout"])
        | select(
            lambda x: {
                "queue_id": int(x[1]),
                "payout": int(x[0]["payout"]),
                "expiry_price": int(x[0]["expirationPrice"]),
                "close_time": int(x[0]["expirationTime"]),
            }
        )
    )

    logger.info(f"payload: {payload}")

    # update_db(payload)


def update_db(payload):
    # print(open_keeper_account.address, brownie.network.chain.id)
    reqUrl = f"https://oracle.buffer-finance-api.link/instant-trading/trade/unlock/?user_signature={keeper_signature()}&environment={brownie.network.chain.id}"
    logger.info(f"resolve_queued_trades_v2: {reqUrl}")
    r = requests.post(url=reqUrl, json=payload)
    print(reqUrl, r.json())
    # print(r.json())

    # return r.json()


def resolve_queued_trades_v2(environment):
    # print(open_keeper_account.address, brownie.network.chain.id)
    reqUrl = f"https://oracle.buffer-finance-api.link/instant-trading/trades/queued/?user_signature={keeper_signature()}&user_address={open_keeper_account.address}&environment={brownie.network.chain.id}"
    logger.info(f"resolve_queued_trades_v2: {reqUrl}")
    r = requests.get(reqUrl)
    # print(reqUrl, r.json())
    print(r.json())

    queued_trades = list(
        r.json()
        | where(lambda x: x["is_limit_order"] == False and x["state"] == "QUEUED")
    )
    # print("data", queued_trades)
    _resolve_queued_trades(queued_trades, environment=environment)


def has_expired(trades):
    return True


def keeper_signature():
    web3 = brownie.network.web3
    key = open_keeper_account.private_key
    message = encode_defunct(text="Sign to verify user address")
    signed_message = Account.sign_message(message, key)

    def to_32byte_hex(val):
        return web3.toHex(web3.toBytes(val).rjust(32, b"\0"))

    return to_32byte_hex(signed_message.signature)
