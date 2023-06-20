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
from data import (
    fetch_prices,
    get_asset_pair,
    get_expired_options,
    get_limit_orders,
    get_market_info,
    get_option_to_expire,
    get_options_contract,
    get_router_contract,
    get_sf,
    keeper_signature,
)
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


def _(x):
    return json.dumps(x, indent=4, sort_keys=True)


def get_target_contract_mapping(d, environment):
    # Filter out the ones for invalid pairs
    target_option_contracts_mapping = list(
        d
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
    logger.info(f"target_option_contracts_mapping: {(target_option_contracts_mapping)}")

    return target_option_contracts_mapping


def _unlock_options(expired_options, environment):
    if not expired_options:
        return

    # Filter out the ones for invalid pairs
    target_option_contracts_mapping = get_target_contract_mapping(
        expired_options, environment
    )

    # Take the initial 100
    expired_options = expired_options[:MAX_BATCH_SIZE]
    router = get_router_contract(config.ROUTER[environment])
    options_contracts = {
        x["contractAddress"]: get_options_contract(x["contractAddress"])
        for x in expired_options
    }

    brownie.multicall(address=config.MULTICALL[environment])

    with brownie.multicall:
        # Confirm if these options are still active by using RPC calls
        option_details = [
            options_contracts[x["contractAddress"]].options(x["optionID"])
            for x in expired_options
        ]
        queue_ids = [
            router.optionIdMapping(x["contractAddress"], x["optionID"])
            for x in expired_options
        ]
    expired_options = list(zip(expired_options, option_details, queue_ids))
    market_info = get_market_info(environment)

    expired_options = list(
        expired_options
        | where(lambda x: x[1][0] == 1 and market_info.get(x[2]))
        | select(
            lambda x: {
                "contractAddress": x[0]["contractAddress"],
                "optionID": x[0]["optionID"],
                "expirationTime": x[0]["expirationTime"],
                "queueId": x[2],
            }
        )
    )

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

    logger.info(f"prices_to_fetch: {_(prices_to_fetch)}")
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
                # x["queueId"],
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
        params = {
            "from": close_keeper_account,
            "gas": config.GAS_PRICE[environment],
            "required_confs": int(os.environ["CONFS"]),
            "max_fee": (2 * brownie.chain.base_fee) + brownie.chain.priority_fee,
            "priority_fee": brownie.chain.priority_fee,
            "allow_revert": True,
        }
        gas = router.executeOptions.estimate_gas(unlock_payload, params) * 1.01

        logger.info(f"Transacting at {gas} gas units...")
        try:
            router.executeOptions(
                unlock_payload,
                {**params, "gas_limit": gas},
            )
            check_wallet(close_keeper_account)
        except Exception as e:
            if "nonce too low" in str(e):
                logger.info(e)
            else:
                logger.exception(e)


def is_strike_valid(slippage, current_price, strike):
    if (current_price <= (strike * (1e4 + slippage)) / 1e4) and (
        current_price >= (strike * (1e4 - slippage)) / 1e4
    ):
        return True
    else:
        return False


def update_db_for_lo(payload, environment):
    reqUrl = f"{config.BASE_URL}/trade/create/limit_order/?user_signature={keeper_signature()}&environment={brownie.network.chain.id}"
    logger.info(f"resolve_queued_trades_v2: {reqUrl}")
    r = requests.post(url=reqUrl, json=payload)


def open_limit_orders(environment):
    logger.info(f"{mp.current_process().name} {datetime.now()}")
    lo = get_limit_orders(environment)
    lo = list(
        lo
        | select(
            lambda x: {
                **x,
                "contractAddress": x["target_contract"],
            }
        )
    )
    if not lo:
        return
    # Filter out the ones for invalid pairs
    target_option_contracts_mapping = get_target_contract_mapping(lo, environment)
    # Take the initial 100
    lo = lo[:MAX_BATCH_SIZE]
    if not lo:
        return

    current_time = int(time.time())
    prices_to_fetch = list(
        target_option_contracts_mapping
        | select(
            lambda x: {
                "pair": target_option_contracts_mapping[x],
                "timestamp": current_time,
            }
        )
    )  # List[(assetPair, timestamp)]

    logger.info(f"prices_to_fetch: {_(prices_to_fetch)}")
    fetched_prices_mapping = fetch_prices(prices_to_fetch)
    logger.info(f"fetched_prices_mapping: {(fetched_prices_mapping)}")
    _price = lambda x: fetched_prices_mapping.get(
        f"{target_option_contracts_mapping[x['contractAddress']]}-{current_time}",
        {},
    )
    sf = get_sf(environment)
    _sf = lambda x: sf.get(
        f"{target_option_contracts_mapping[x['contractAddress']]}",
        {},
    )

    valid_orders = list(
        lo
        | where(
            lambda x: is_strike_valid(
                x["slippage"], _price(x).get("price"), x["strike"]
            )
            and (x["limit_order_duration"] + x["queued_timestamp"]) > current_time
        )
        | select(
            lambda x: (
                x["queue_id"],  # queueId
                x["user_address"],  # timestamp,
                x["trade_size"],  # price,
                x["period"],  # signature,
                x["target_contract"],  # signature,
                x["strike"],
                x["slippage"],
                x["allow_partial_fill"],
                x["referral_code"],
                x["trader_nft_id"],
                _price(x)["price"],  # price,
                _sf(x)["settlement_fee"],
                x["is_limit_order"],
                x["limit_order_duration"] + x["queued_timestamp"],
                [
                    _sf(x)["settlement_fee_signature"],
                    _sf(x)["settlement_fee_sign_expiration"],
                ],  # signature,
                [x["user_partial_signature"], x["signature_timestamp"]],
                [_price(x)["signature"], current_time],  # signature
            )
        )
    )

    if valid_orders:
        logger.info(f"valid_orders : {_(valid_orders)}")
        router_contract = get_router_contract(config.ROUTER[environment])
        params = {
            "from": open_keeper_account,
            "gas": config.GAS_PRICE[environment],
            "required_confs": int(os.environ["CONFS"]),
            "allow_revert": True,
            "max_fee": (2 * brownie.chain.base_fee) + brownie.chain.priority_fee,
            "priority_fee": brownie.chain.priority_fee,
        }
        gas = router_contract.openTrades.estimate_gas(valid_orders, params) * 1.01
        logger.info(f"Transacting at {gas}  gas units...")
        try:
            router_contract.openTrades(valid_orders, {**params, "gas_limit": gas})
            check_wallet(open_keeper_account)
        except Exception as e:
            if "nonce too low" in str(e):
                logger.info(e)
            else:
                logger.exception(e)

        queue_ids = [x[0] for x in valid_orders]
        update_db_for_lo(queue_ids, environment)

    invalid_orders = list(
        lo
        | where(
            lambda x: (x["limit_order_duration"] + x["queued_timestamp"]) < current_time
        )
        | select(lambda x: x["queue_id"])  # queueId
    )
    if invalid_orders:
        logger.info(f"invalid_orders : {_(invalid_orders)}")
        cancel_trades(invalid_orders, environment)


def cancel_trades(payload, environment):
    reqUrl = f"{config.BASE_URL}/trades/cancel/?user_signature={keeper_signature()}&user_address={open_keeper_account}&environment={brownie.network.chain.id}"
    logger.info(f"resolve_queued_trades_v2: {reqUrl}")
    r = requests.post(url=reqUrl, json=payload)
    print(reqUrl, r.json())


def unlock_options_v2(environment):
    logger.info(f"{mp.current_process().name} {datetime.now()}")
    expired_options = get_option_to_expire(environment)
    _unlock_options(
        expired_options,
        environment=environment,
    )


def update_db_after_unlock(environment):
    logger.info(f"{mp.current_process().name} {datetime.now()}")
    graph_data, db_data = get_expired_options(environment)
    if not (graph_data and db_data):
        return
    router = get_router_contract(config.ROUTER[environment])

    brownie.multicall(address=config.MULTICALL[environment])
    with brownie.multicall:
        queue_ids = [
            router.optionIdMapping(x["contractAddress"], x["optionID"])
            for x in graph_data
        ]

    payload = list(
        zip(graph_data, queue_ids)
        | where(lambda x: db_data.get(int(x[1])))
        | select(
            lambda x: {
                "queue_id": int(x[1]),
                "payout": int(x[0]["payout"]) if x[0]["payout"] else 0,
                "expiry_price": int(x[0]["expirationPrice"]),
                "close_time": int(x[0]["closeTime"]),
            }
        )
    )

    logger.info(f"payload: {payload}")
    if payload:
        update_db(payload)


def update_db(payload):
    reqUrl = f"{config.BASE_URL}/trade/unlock/?user_signature={keeper_signature()}&environment={brownie.network.chain.id}"
    logger.info(f"resolve_queued_trades_v2: {reqUrl}")
    r = requests.post(url=reqUrl, json=payload)
    print(reqUrl, r.json())
