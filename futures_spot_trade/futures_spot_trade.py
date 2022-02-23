# -*- coding: UTF-8 -*-
import re
import copy
import sys
import threading
from multiprocessing import Process
import time
import traceback
import json

from utils.redis_util import master_redis_client, redis_client
from utils.const import const
from strategy_order.utils.logger_cfg import futures_spot_trade_log
from utils.config import config as configBase
from futures_spot_trade.futures_spot_trade import Exchanges as exchanges
from huobi.api import HuobiSpot, HuobiAccount, HuobiFutures
from okex.api_v3 import OkexRequest, OkexSpot, OkexFutures, OkexAccount
from utils.function_util import *

_logger = futures_spot_trade_log


class FuturesSpotTrade():
    ''' 期现策略交易 '''

    def __init__(self):
        self.master_redis_client = master_redis_client
        self.exchanges = exchanges()
        self.redis_client = redis_client
        self.api_params = {}
        self.symbols_dict = {'HB': {'futures': {}, 'spot': {}}, 'OK': {'futures': {}, 'spot': {}}}

    def get_url(self):
        ''' 获取redis接收url '''
        try:
            passwod = configBase.redis_master_password
            if not passwod:
                url = "redis://{}:{}/{}".format(configBase.redis_master_host, configBase.redis_master_port,
                                                configBase.redis_master_db)
            else:
                url = "redis://:{}@{}:{}/{}".format(passwod, configBase.redis_master_host,
                                                    configBase.redis_master_port, configBase.redis_master_db)
            return url
        except Exception as e:
            _logger.error('----- get_url, {} -----'.format(traceback.format_exc()))

    def get_lasted_price(self, strategy_params):  # 合约的固定deposit_type是USD
        ''' 最新价 '''
        entrust_type = strategy_params.get('entrust_type', '')
        market_price = 0

        try:
            _logger.info('----- get_lasted_price, 正在获取最新价 -----')
            if entrust_type == '合约':
                exchange_name = strategy_params.get('exchange', '').upper()
                currency = strategy_params.get('currency', '')
                contract_type = strategy_params.get('contract_type', '')
                deposit_type = 'USD'  # 合约默认都是USD

                if re.search('OK', exchange_name):
                    exchange_name = 'OK'

                if re.search('HB', exchange_name):
                    exchange_name = 'HB'

                if deposit_type.upper() == 'USDT':
                    key = '{}.{}.{}.{}.trade.price'.format(exchange_name, currency, contract_type, deposit_type)
                else:
                    key = '{}.{}.{}.trade.price'.format(exchange_name, currency, contract_type)

                key = re.sub('this_', '', key)
                value = self.redis_client.get(key)

                if value: market_price = float(json.loads(value))
                _logger.info('----- get_lasted_price, 合约, key: {}, value: {} -----'.format(key, value))
            elif entrust_type == '现货':
                pair_id = self.exchanges.get_pair_id(strategy_params)
                value = self.redis_client.get(pair_id)

                if value: market_price = float(json.loads(value))
                _logger.info('----- get_lasted_price, 现货, pair_id: {}, value: {} -----'.format(pair_id, value))
        except Exception as e:
            _logger.error('----- get_lasted_price, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info('----- get_lasted_price, strategy_params: {}, market_price: {} -----'.format(strategy_params,
                                                                                                      market_price))
            return market_price

    def get_market_price(self):
        ''' 市价 '''
        return 0

    def init_api_params(self, strategy_params):
        try:
            api_config_params = self.exchanges.get_api_config_params(strategy_params)

            id = strategy_params.get('id', '')
            if id not in self.api_params.keys():
                exchange = strategy_params.get('exchange', '')

                if exchange.upper() == 'OK':
                    access_key = api_config_params.get('access_key', '')
                    secret_key = api_config_params.get('secret_key', '')
                    pass_phrase = api_config_params.get('pass_phrase', '')

                    req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                                      use_server_time=True)

                    okex_futures = OkexFutures(req)
                    okex_spot = OkexSpot(req)
                    okex_account = OkexAccount(req)

                    self.api_params[id] = {'okex_spot': okex_spot,
                                           'okex_futures': okex_futures,
                                           'okex_account': okex_account}

                elif exchange.upper() == 'HB':
                    spot_url = api_config_params.get('spot_url', '')  # 现货URL
                    futures_url = api_config_params.get('futures_url', '')  # 合约URL

                    ACCESS_KEY = api_config_params.get('access_key', '')
                    SECRET_KEY = api_config_params.get('secret_key', '')

                    huobi_futures = HuobiSpot(futures_url, ACCESS_KEY, SECRET_KEY)
                    huobi_spot = HuobiSpot(spot_url, ACCESS_KEY, SECRET_KEY)
                    huobi_account = HuobiAccount(spot_url, ACCESS_KEY, SECRET_KEY)

                    self.api_params[id] = {'huobi_spot': huobi_spot,
                                           'huobi_futures': huobi_futures,
                                           'huobi_account': huobi_account}
        except Exception as e:
            _logger.error(traceback.format_exc())

    def init_ok_symbols_dict(self, strategy_params, api_config_params):
        ''' 初始化ok交易对参数 '''
        try:
            _logger.info('----- init_ok_symbols_dict, strategy_params: {}, api_config_params: {} -----'.format(
                strategy_params, api_config_params))

            strategy_params = copy.copy(strategy_params)
            currency = strategy_params.get('currency', '')
            entrust_type = strategy_params.get('entrust_type', '')

            access_key = api_config_params.get('access_key', '')
            secret_key = api_config_params.get('secret_key', '')
            pass_phrase = api_config_params.get('pass_phrase', '')

            if entrust_type == '现货':
                symbol = str(currency).lower() + 'usdt'
                instrument_id = '{}-{}'.format(str(currency).upper(), 'USDT')

                if symbol in self.symbols_dict['OK']['spot'].keys():
                    return True  # 之前存过不再存

                # -------------------------------------- 查询现货币对信息 ---------------------------------------
                _logger.info(
                    '----- init_ok_symbols_dict, 现货api请求参数, api_key: {}, api_seceret_key: {}, passphrase: {} -----'.format(
                        access_key,
                        secret_key,
                        pass_phrase))
                req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                                  use_server_time=True)
                okex_spot = OkexSpot(req)

                api_result = okex_spot.get_instruments()
                _logger.info('----- init_ok_symbols_dict, 现货api返回结果: {} -----'.format(api_result))

                if api_result:
                    all_datas = list(api_result)
                    for data in all_datas:
                        api_instrument_id = data.get('instrument_id', '')
                        if instrument_id.lower() == api_instrument_id.lower():
                            tick_size = data.get('tick_size', '')
                            size_increment = data.get('size_increment', '')

                            price_precision = len(str(tick_size).split('.')[1])
                            amount_precision = len(str(size_increment).split('.')[1])

                            self.symbols_dict['OK']['spot'][symbol] = {'price_precision': str(price_precision),
                                                                       'amount_precision': str(amount_precision)}
                            break
                # -------------------------------------- 查询现货币对信息 ---------------------------------------
            elif entrust_type == '合约':
                symbol = str(currency).lower() + 'usd'
                instrument_id = '{}-{}'.format(str(currency).upper(), 'USD')

                if symbol in self.symbols_dict['OK']['futures'].keys():
                    return True  # 之前存过不再存

                # -------------------------------------- 查询合约币对信息 ---------------------------------------
                _logger.info(
                    '----- init_ok_symbols_dict, 合约api请求参数, api_key: {}, api_seceret_key: {}, passphrase: {} -----'.format(
                        access_key,
                        secret_key,
                        pass_phrase))
                req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                                  use_server_time=True)
                okex_futures = OkexFutures(req)

                api_result = okex_futures.get_instruments()
                _logger.info('----- init_ok_symbols_dict, 合约api返回结果: {} -----'.format(api_result))

                if api_result:
                    all_datas = list(api_result)
                    for data in all_datas:
                        api_instrument_id = data.get('instrument_id', '')  # BTC-USDT-191227
                        api_instrument_id = '-'.join(str(api_instrument_id).split('-')[: -1])

                        if instrument_id.lower() == api_instrument_id.lower():
                            tick_size = data.get('tick_size', '')

                            price_precision = len(str(tick_size).split('.')[1])  # 0.00001
                            amount_precision = 0

                            self.symbols_dict['OK']['futures'][symbol] = {'price_precision': str(price_precision),
                                                                          'amount_precision': str(amount_precision)}
                            break
                # -------------------------------------- 查询合约币对信息 ---------------------------------------
        except Exception as e:
            _logger.error('----- init_ok_symbols_dict, {} -----'.format(traceback.format_exc()))

    def init_hb_symbols_dict(self, strategy_params, api_config_params):
        ''' 初始化hb交易对参数 '''
        try:
            _logger.info('----- init_hb_symbols_dict, strategy_params: {}, api_config_params: {} -----'.format(
                strategy_params, api_config_params))

            strategy_params = copy.copy(strategy_params)
            symbol = ''
            currency = strategy_params.get('currency', '')
            contract_type = strategy_params.get('contract_type', '')
            entrust_type = strategy_params.get('entrust_type', '')

            URL = api_config_params.get('spot_url', '')  # 现货URL
            ACCESS_KEY = api_config_params.get('access_key', '')
            SECRET_KEY = api_config_params.get('secret_key', '')

            if entrust_type == '现货':
                symbol = str(currency).lower() + 'usdt'

                if symbol in self.symbols_dict['HB']['spot'].keys():
                    return True  # 之前存过不再存

                # -------------------------------------- 查询现货币对信息 ---------------------------------------
                _logger.info(
                    '----- init_hb_symbols_dict, 现货api请求参数, URL: {}, ACCESS_KEY: {}, SECRET_KEY: {} -----'.format(
                        URL,
                        ACCESS_KEY,
                        SECRET_KEY))
                huobi_spot = HuobiSpot(URL, ACCESS_KEY, SECRET_KEY)
                api_result = huobi_spot.get_common_symbols()
                _logger.info('----- init_hb_symbols_dict, 现货api返回结果: {} -----'.format(api_result))

                status = api_result.get('status', '')
                all_datas = api_result.get('data', [])

                if status.lower() == 'ok' and all_datas:
                    for data in all_datas:
                        api_symbol = data.get('symbol', '')

                        if api_symbol.lower() == symbol.lower():
                            _logger.info('----- init_hb_symbols_dict, 查询现货对信息成功, data: {} -----'.format(data))

                            price_precision = data.get('price-precision')
                            amount_precision = data.get('amount-precision')
                            self.symbols_dict['HB']['spot'][symbol] = {'price_precision': str(price_precision),
                                                                       'amount_precision': str(amount_precision)}
                            break
                # -------------------------------------- 查询现货币对信息 ---------------------------------------
            elif entrust_type == '合约':
                symbol = str(currency).lower() + 'usd'

                if symbol in self.symbols_dict['HB']['futures'].keys():
                    return True  # 之前存过不再存

                # -------------------------------------- 查询合约币对信息 ---------------------------------------
                URL = api_config_params.get('futures_url', '')  # 合约URL
                _logger.info(
                    '----- init_hb_symbols_dict, 合约api请求参数, URL: {}, ACCESS_KEY: {}, SECRET_KEY: {}, currency: {},contract_type: {}  -----'.format(
                        URL, ACCESS_KEY, SECRET_KEY, currency, contract_type))
                huobi_futures = HuobiFutures(URL, ACCESS_KEY, SECRET_KEY)
                api_result = huobi_futures.get_contract_contract_info(symbol=currency)
                _logger.info('----- init_hb_symbols_dict, 合约api返回结果: {} -----'.format(api_result))

                status = api_result.get('status', '')
                all_datas = api_result.get('data', [])

                if status.lower() == 'ok' and all_datas:
                    for data in all_datas:
                        api_symbol = data.get('symbol', '')
                        if api_symbol.lower() == currency.lower():
                            _logger.info('----- init_hb_symbols_dict, 查询合约对信息成功, data: {} -----'.format(data))

                            price_tick = data.get('price_tick', '')

                            price_precision = len(str(price_tick).split('.')[1])
                            amount_precision = 0  # 合约张数为单位
                            self.symbols_dict['HB']['futures'][symbol] = {'price_precision': str(price_precision),
                                                                          'amount_precision': str(amount_precision)}
                            break
                # -------------------------------------- 查询合约币对信息 ---------------------------------------
        except Exception as e:
            _logger.error('----- init_hb_symbols_dict, {} -----'.format(traceback.format_exc()))

    def ok_spot_place_order(self, strategy_params, api_config_params):
        ''' ok现货下单 '''
        place_result, order_id = False, ''
        try:
            _logger.info('----- ok_spot_place_order, OK正在现货下单, strategy_params: {}, api_config_params: {} -----'.format(
                strategy_params, api_config_params))

            strategy_params = copy.copy(strategy_params)
            account_id = strategy_params.get('account_id', '')
            currency = strategy_params.get('currency', '')
            trade_price = strategy_params.get('trade_price')  # 委托价格
            trade_amount = strategy_params.get('trade_amount')  # 委托数量

            access_key = api_config_params.get('access_key', '')
            secret_key = api_config_params.get('secret_key', '')
            pass_phrase = api_config_params.get('pass_phrase', '')
            instrument_id = '{}-{}'.format(currency, 'USDT')

            _type = ''
            _notional = ''
            order_trade_type = ''

            type = strategy_params.get('type', '')

            if type == 'buy':  # 开仓单
                buy_type = strategy_params.get('buy_type', '')
                order_trade_type = '现货买入'

                if buy_type == '最新价':  # 限价下单
                    _type = 'limit'
                elif buy_type == '市价':  # 市价下单
                    _type = 'market'
                    _notional = strategy_params.get('buy_amount', '')
                elif buy_type == '对手价':  # 现货对手价下单, 默认市价下单
                    _type = 'market'
                    _notional = strategy_params.get('buy_amount', '')

            elif type == 'sell':  # 平仓单
                sell_type = strategy_params.get('sell_type', '')
                order_trade_type = '现货卖出'

                if sell_type == '最新价':  # 限价下单
                    _type = 'limit'
                elif sell_type == '市价':  # 市价下单
                    _type = 'market'
                    _notional = strategy_params.get('sell_amount', '')
                elif sell_type == '对手价':  # 现货对手价下单, 默认市价下单
                    _type = 'market'
                    _notional = strategy_params.get('sell_amount', '')

            # ---------------------------------------- 下单部分 ------------------------------------------------------
            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_spot = OkexSpot(req)

            _logger.info('----- ok_spot_place_order, api请求参数, instrument_id: {}, '
                         'type: {}, size: {}, '
                         'side: {}, price: {}, '
                         'notional: {} -----'.format(
                instrument_id, _type, trade_amount, type, trade_price, _notional))
            api_result = okex_spot.place_order(instrument_id=instrument_id.upper(),
                                               type=_type,
                                               size=trade_amount,
                                               side=type,
                                               price=trade_price,
                                               notional=_notional)
            _logger.info('----- ok_spot_place_order, api返回结果: {} -----'.format(api_result))

            error_code = api_result.get('error_code', '')
            error_message = api_result.get('error_message', '')

            trade_price = self.exchanges.handler_entrust_price(trade_price)  # 更新委托价格

            # 现货市价买入委托数量 + USDT
            if type == 'buy':
                if trade_price == '市价':
                    trade_amount = str(trade_amount) + 'USDT'
                    strategy_params['trade_amount'] = trade_amount  # 更新委托数量

            # 下单成功, 存委托表
            if str(error_code) == '0':
                order_id = api_result.get('order_id')

                strategy_params['order_id'] = order_id
                update_result = self.exchanges.update_t_futures_spot_entrust(strategy_params)  # 存委托表

                if not update_result:
                    _logger.error('----- ok_spot_place_order, 保存委托表失败 -----')
                    return

                # -------------------------------- 推送现货下单日志 ---------------------------------------------
                strategy_params['information'] = '{}，{}，委托价格：{}，委托数量：{}'.format(order_trade_type, currency, trade_price,
                                                                                trade_amount)  # order_trade_type
                strategy_params['log_type'] = 'place'
                self.exchanges.push_log(strategy_params, 'log')
                # -------------------------------- 推送现货下单日志 ---------------------------------------------

                place_result = True
            else:  # 下单失败
                # -------------------------------- 推送现货下单失败日志 ---------------------------------------------
                information = '现货下单失败，{}。[委托价格：{}，委托数量：{}]'.format(error_message, trade_price, trade_amount)
                strategy_params['information'] = information
                strategy_params['log_type'] = 'error'
                self.exchanges.push_log(strategy_params, 'log')
                # -------------------------------- 推送现货下单失败日志 ---------------------------------------------

                # ------------------------------- 存下单失败操作日志 --------------------------------------------
                self.exchanges.futures_spot_save_operator_log(strategy_params, '失败')
                # ------------------------------- 存下单失败操作日志 --------------------------------------------
            # ---------------------------------------- 下单部分 ------------------------------------------------------

        except Exception as e:
            _logger.error('----- ok_spot_place_order, {} -----'.format(traceback.format_exc()))
        finally:
            return place_result, order_id

    def hb_spot_place_order(self, strategy_params, api_config_params):
        ''' hb现货下单 '''
        place_result, order_id = False, ''
        try:
            _logger.info('----- hb_spot_place_order, HB正在现货下单, strategy_params: {}, api_config_params: {} -----'.format(
                strategy_params, api_config_params))

            strategy_params = copy.copy(strategy_params)
            account_id = strategy_params.get('account_id', '')
            currency = strategy_params.get('currency', '')
            trade_price = strategy_params.get('trade_price')  # 委托价格
            trade_amount = strategy_params.get('trade_amount')  # 委托数量

            URL = api_config_params.get('spot_url', '')  # 现货URL
            ACCESS_KEY = api_config_params.get('access_key', '')
            SECRET_KEY = api_config_params.get('secret_key', '')

            _type = ''
            order_trade_type = ''

            symbol = currency.lower() + 'usdt'
            type = strategy_params.get('type', '')

            if type == 'buy':  # 开仓单
                buy_type = strategy_params.get('buy_type', '')
                order_trade_type = '现货买入'

                if buy_type == '最新价':  # 限价下单
                    _type = 'buy-limit'
                elif buy_type == '市价':  # 市价下单
                    _type = 'buy-market'
                elif buy_type == '对手价':  # 现货对手价下单, 默认市价下单
                    _type = 'buy-market'
            elif type == 'sell':  # 平仓单
                sell_type = strategy_params.get('sell_type', '')
                order_trade_type = '现货卖出'

                if sell_type == '最新价':  # 限价下单
                    _type = 'sell-limit'
                elif sell_type == '市价':  # 市价下单
                    _type = 'sell-market'
                elif sell_type == '对手价':  # 现货对手价下单, 默认市价下单
                    _type = 'sell-market'
            # ---------------------------------------- _account_id调接口查 ------------------------------------------
            huobi_account = HuobiAccount(URL, ACCESS_KEY, SECRET_KEY)
            data = huobi_account.get_accounts()
            _account_id = ''
            _amount = trade_amount

            if data:
                _account_id = data.get('data')[0].get('id', '')
            _logger.info('----- hb_spot_place_order, _account_id: {}, _type: {} -----'.format(_account_id, _type))
            # ---------------------------------------- _account_id调接口查 ------------------------------------------

            # ---------------------------------------- 下单部分 ------------------------------------------------------
            huobi_spot = HuobiSpot(URL, ACCESS_KEY, SECRET_KEY)
            _logger.info('----- hb_spot_place_order, api请求参数, _account_id: {}, '
                         'symbol: {}, _type: {}, '
                         'amount: {}, price: {}, '
                         'source: {} -----'.format(
                _account_id, symbol, _type, _amount, trade_price, 'spot-api'))

            orders_data = [
                {
                    "account-id": str(_account_id),
                    "price": trade_price,
                    "amount": _amount,
                    "symbol": symbol,
                    "type": _type,
                    'source': 'spot-api'
                }
            ]
            api_result = huobi_spot.send_spot_batch_orders(orders_data)
            status = api_result.get('status', '')

            _logger.info('----- hb_spot_place_order, api返回结果: {} -----'.format(api_result))

            # 下单成功, 存委托表
            if status.lower() == 'ok':
                api_result_datas = api_result.get('data', [])

                for api_result_data in api_result_datas:
                    # 判断是否被拒
                    err_code = api_result_data.get('err-code', '')
                    err_msg = api_result_data.get('err-msg', '')

                    trade_price = self.exchanges.handler_entrust_price(trade_price)  # 更新委托价格

                    # 现货市价买入委托数量 + USDT
                    if type == 'buy':
                        if trade_price == '市价':
                            trade_amount = str(trade_amount) + 'USDT'
                            strategy_params['trade_amount'] = trade_amount  # 更新委托数量

                    if not err_code and not err_msg:  # 下单成功
                        order_id = api_result_data.get('order-id', '')

                        strategy_params['order_id'] = order_id
                        update_result = self.exchanges.update_t_futures_spot_entrust(strategy_params)  # 存委托表

                        if not update_result:
                            _logger.error('----- hb_spot_place_order, 保存委托表失败 -----')
                            continue

                        # -------------------------------- 推送现货下单日志 ---------------------------------------------
                        strategy_params['information'] = '{}，{}，委托价格：{}，委托数量：{}'.format(order_trade_type, currency,
                                                                                        trade_price,
                                                                                        trade_amount)
                        strategy_params['log_type'] = 'place'
                        self.exchanges.push_log(strategy_params, 'log')
                        # -------------------------------- 推送现货下单日志 ---------------------------------------------

                        place_result = True
                    else:  # 下单失败
                        # -------------------------------- 推送现货下单失败日志 ---------------------------------------------
                        information = '现货下单失败，{}。[委托价格：{}，委托数量：{}]'.format(err_msg, trade_price, trade_amount)
                        strategy_params['information'] = information
                        strategy_params['log_type'] = 'error'
                        self.exchanges.push_log(strategy_params, 'log')
                        # -------------------------------- 推送现货下单失败日志 ---------------------------------------------

                        # ------------------------------- 存下单失败操作日志 --------------------------------------------
                        self.exchanges.futures_spot_save_operator_log(strategy_params, '失败')
                        # ------------------------------- 存下单失败操作日志 --------------------------------------------
            # ---------------------------------------- 下单部分 ------------------------------------------------------
        except Exception as e:
            _logger.error('----- hb_spot_place_order, {} -----'.format(traceback.format_exc()))
        finally:
            return place_result, order_id

    def monitor_ok_spot_order_status(self, strategy_params, api_config_params, order_id, symbol):
        ''' 循环监听ok现货订单 '''
        deal_result = False  # 成交结果
        try:
            _logger.info('----- monitor_ok_spot_order_status, strategy_params: {}, api_config_params: {}, order_id: {} '
                         '-----'.format(strategy_params, api_config_params, order_id))

            price_precision = self.symbols_dict['OK']['spot'].get(symbol, {}).get('price_precision', '0')
            amount_precision = self.symbols_dict['OK']['spot'].get(symbol, {}).get('amount_precision', '0')

            strategy_params = copy.copy(strategy_params)
            currency = strategy_params.get('currency', '')
            trade_price = strategy_params.get('trade_price')  # 委托价格
            trade_amount = strategy_params.get('trade_amount')  # 委托数量

            access_key = api_config_params.get('access_key', '')
            secret_key = api_config_params.get('secret_key', '')
            pass_phrase = api_config_params.get('pass_phrase', '')

            instrument_id = '{}-{}'.format(currency.upper(), 'USDT')

            _logger.info('----- monitor_ok_spot_order_status, api请求参数, api_key: {}, api_seceret_key: {}, '
                         'passphrase: {}, order_id: {} -----'.format(access_key, secret_key, pass_phrase, order_id))
            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_spot = OkexSpot(req)

            while True:
                api_result = okex_spot.get_order_detail(instrument_id=instrument_id.upper(), order_id=order_id)
                _logger.info('----- monitor_ok_spot_order_status, api_result: {}----'.format(api_result))
                state = api_result.get('state', '')

                # ----------------------------- 获取订单信息 -----------------------------------------
                entrust_time = ''
                order_trade_type = ''
                entrust_type = strategy_params.get('entrust_type', '')
                _type = strategy_params.get('type', '')
                account_name = strategy_params.get('account_name')
                account_id = strategy_params.get('account_id')

                timestamp = api_result.get('timestamp', '')  # 订单创建时间
                deal_amount = api_result.get('filled_size', '')  # 成交数量
                if deal_amount: deal_amount = self.exchanges.fill_num(deal_amount, amount_precision)
                deal_price = api_result.get('price_avg', '')  # 成交价格
                if deal_price: deal_price = self.exchanges.fill_num(deal_price, price_precision)
                deal_fee = api_result.get('fee', '')  # 成交手续费
                if deal_fee: deal_fee = self.exchanges.fill_num(-abs(float(deal_fee)), 6)  # 默认均负数
                # ----------------------------- 获取订单信息 -----------------------------------------

                trade_price = self.exchanges.handler_entrust_price(trade_price)  # 更新委托价格
                order_trade_type = '现货买入' if _type == 'buy' else '现货卖出'

                # -------------------------------- 针对现货买入委托数量, 现货卖出成交手续费+USDT --------------------------------
                if _type == 'buy':
                    if trade_price == '市价':
                        trade_amount = str(trade_amount) + 'USDT'
                        strategy_params['trade_amount'] = trade_amount
                elif _type == 'sell':
                    deal_fee = str(deal_fee) + 'USDT'  # 现货卖出, 手续费均加USDT
                # -------------------------------- 针对现货买入委托数量, 现货卖出成交手续费+USDT --------------------------------

                if str(state) == '2':  # 已成交, 全部成交
                    last_arbitrage_time = deal_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                    # 获取最新邮箱
                    delivery_id, stock_id = self.exchanges.get_user_id(account_id)
                    _logger.info(
                        '----- monitor_ok_spot_order_status, account_id: {}, delivery_id: {}, stock_id: {} -----'.format(
                            account_id, delivery_id, stock_id))

                    search_dict = get_table_data(strategy_mysql_conn, const.T_FUTURES_SPOT_ENTRUST, ['entrust_time'],
                                                 ['order_id'], [order_id])  # 取委托时间
                    if search_dict:
                        entrust_time = search_dict.get('entrust_time', '')
                        entrust_time = entrust_time.strftime('%Y-%m-%d %H:%M:%S')

                    recipients = self.exchanges.send_detail_email.get_account_emails(delivery_id)  # 每次发邮件前更新邮箱

                    if _type == 'buy':
                        buy_spot_amount = float(strategy_params.get('buy_spot_amount', 0)) + float(deal_amount)
                        strategy_params['buy_spot_amount'] = buy_spot_amount  # 更新买入现货数量
                        strategy_params['spot_purchase_cost'] = deal_price  # 更新买入现货成本(成交均价)
                        _logger.info(
                            '------ monitor_ok_spot_order_status, buy, 更新买入现货数量, buy_spot_amount: {} -----'.format(
                                buy_spot_amount))

                    elif _type == 'sell':
                        buy_spot_amount = float(strategy_params.get('buy_spot_amount', 0)) - float(deal_amount)
                        strategy_params['buy_spot_amount'] = buy_spot_amount  # 更新买入现货数量
                        _logger.info(
                            '------ monitor_ok_spot_order_status, sell, 更新买入现货数量, buy_spot_amount: {} -----'.format(
                                buy_spot_amount))

                    self.exchanges.update_t_futures_spot_postion(strategy_params, last_arbitrage_time)  # 更新持仓表
                    self.exchanges.save_t_futures_spot_deal(strategy_params, order_id, deal_price, deal_amount,
                                                            deal_fee, entrust_type, entrust_time, deal_time)  # 存成交表

                    # -------------------------------- 推送现货成交日志 ---------------------------------------------
                    strategy_params['information'] = '{}，{}，委托价格：{}，委托数量：{}，成交价格：{}，成交数量：{}，手续费：{}'.format(
                        order_trade_type,
                        currency,
                        trade_price,
                        trade_amount,
                        deal_price,
                        deal_amount,
                        deal_fee)
                    strategy_params['log_type'] = 'deal'
                    self.exchanges.push_log(strategy_params, 'log')
                    # -------------------------------- 推送现货成交日志 ---------------------------------------------

                    # ----------------------------- 发送全部成交邮件 ---------------------------------------
                    email_detail = '账户名称：{}\r\n现货名称：{}\r\n交易类型：{}\r\n委托价格：{}\r\n成交价格：{}\r\n委托数量：{}\r\n成交数量：{}\r\n委托时间：{}\r\n成交时间：{}\r\n手续费：{}\r\n订单编号：{}'.format(
                        account_name,
                        currency.upper(),
                        order_trade_type,
                        trade_price,
                        deal_price,
                        trade_amount,
                        deal_amount,
                        entrust_time,
                        last_arbitrage_time,
                        deal_fee,
                        order_id)

                    email_detail = self.exchanges.handler_email_content(email_detail)
                    self.exchanges.send_grid_trade_email(email_detail, recipients, '期现策略：您有新的成交记录')
                    _logger.info(
                        '----- 发送期现交易成交邮件成功, recipients: {}, email_detail: {} -----'.format(recipients, email_detail))
                    # ----------------------------- 发送全部成交邮件 ---------------------------------------

                    deal_result = True  # 全部成交, 更新
                    break

                elif str(state) == '1':  # 部分成交
                    strategy_params['part_deal_amount'] = deal_amount  # 部分成交数量
                    strategy_params['part_deal_price'] = deal_price  # 部分成交价格
                    strategy_params['order_id'] = order_id

                    self.exchanges.update_t_futures_spot_entrust(strategy_params)  # 更新委托成交数量

                elif str(state) == '-1':  # -1 撤单成功
                    _logger.info(
                        '----- monitor_ok_spot_order_status, 监听到已撤单， order_id: {}, order_status: {} -----'.format(
                            order_id, state))

                    # 删除委托
                    delete_result = self.exchanges.delete_entrust_data(order_id)
                    if delete_result:
                        # 推送已撤单日志
                        strategy_params['information'] = '{}，委托价格：{}，委托数量：{}，订单编号：{}'.format(
                            order_trade_type,
                            trade_price,
                            trade_amount,
                            order_id)
                        strategy_params['log_type'] = 'cancel'
                        self.exchanges.push_log(strategy_params, 'log')

                    break  # 退出循环

                elif str(state) == '-2':  # 失败
                    _logger.info(
                        '----- monitor_ok_spot_order_status, 监听到已失败， order_id: {}, order_status: {} -----'.format(
                            order_id, state))

                    # 删除委托
                    delete_result = self.exchanges.delete_entrust_data(order_id)
                    if delete_result:
                        # 推送已失败日志
                        strategy_params['information'] = '{}订单失败，委托价格：{}，委托数量：{}，订单编号：{}'.format(
                            order_trade_type,
                            trade_price,
                            trade_amount,
                            order_id)
                        strategy_params['log_type'] = 'info'
                        self.exchanges.push_log(strategy_params, 'log')

                    break  # 退出循环

                # 轮询间隔时间
                futures_spot_order_query_interval = self.exchanges.get_strategy_param().get(
                    'futures_spot_order_query_interval', 1)
                time.sleep(int(futures_spot_order_query_interval))  # 每隔一段时间调接口查是否全部成交
        except Exception as e:
            _logger.error('----- monitor_ok_spot_order_status, {} -----'.format(traceback.format_exc()))
        finally:
            return deal_result

    def monitor_hb_spot_order_status(self, strategy_params, api_config_params, order_id, symbol):
        ''' 循环监听hb现货订单 '''
        deal_result = False  # 成交结果
        try:
            _logger.info('----- monitor_hb_spot_order_status, strategy_params: {}, api_config_params: {}, order_id: {} '
                         '-----'.format(strategy_params, api_config_params, order_id))

            price_precision = self.symbols_dict['HB']['spot'].get(symbol, {}).get('price_precision', '0')
            amount_precision = self.symbols_dict['HB']['spot'].get(symbol, {}).get('amount_precision', '0')

            strategy_params = copy.copy(strategy_params)

            URL = api_config_params.get('spot_url', '')  # 现货URL
            ACCESS_KEY = api_config_params.get('access_key', '')
            SECRET_KEY = api_config_params.get('secret_key', '')

            _logger.info('----- monitor_hb_spot_order_status, api请求参数, URL: {}, ACCESS_KEY: {}, '
                         'SECRET_KEY: {}, order_id: {} -----'.format(URL, ACCESS_KEY, SECRET_KEY, order_id))
            huobi_spot = HuobiSpot(URL, ACCESS_KEY, SECRET_KEY)

            while True:
                api_result = huobi_spot.get_order_detail(order_id)
                _logger.info('----- monitor_hb_spot_order_status, api_result: {} -----'.format(api_result))

                status = api_result.get('status', '')
                data = api_result.get('data', {})
                state = data.get('state', '')

                if status.lower() == 'ok':
                    # ----------------------------- 获取订单信息 -----------------------------------------
                    entrust_time = ''
                    order_trade_type = ''
                    entrust_type = strategy_params.get('entrust_type', '')
                    currency = strategy_params.get('currency', '')
                    _type = strategy_params.get('type', '')
                    account_name = strategy_params.get('account_name')
                    account_id = strategy_params.get('account_id')
                    trade_price = strategy_params.get('trade_price')  # 委托价格
                    trade_amount = strategy_params.get('trade_amount')  # 委托数量

                    create_at = data.get('created-at', '')  # 订单创建时间
                    deal_amount = data.get('field-amount', 0)  # 成交数量
                    if deal_amount: deal_amount = self.exchanges.fill_num(deal_amount, amount_precision)
                    deal_fee = data.get('field-fees', '')  # 成交手续费
                    if deal_fee: deal_fee = self.exchanges.fill_num(-abs(float(deal_fee)), 6)
                    # ----------------------------- 获取订单信息 -----------------------------------------

                    trade_price = self.exchanges.handler_entrust_price(trade_price)  # 更新委托价格
                    order_trade_type = '现货买入' if _type == 'buy' else '现货卖出'

                    # -------------------------------- 针对现货买入委托数量, 现货卖出成交手续费+USDT --------------------------------
                    if _type == 'buy':
                        if trade_price == '市价':
                            trade_amount = str(trade_amount) + 'USDT'
                            strategy_params['trade_amount'] = trade_amount
                    elif _type == 'sell':
                        deal_fee = str(deal_fee) + 'USDT'  # 现货卖出, 手续费均加USDT
                    # -------------------------------- 针对现货买入委托数量, 现货卖出成交手续费+USDT --------------------------------

                    if state.lower() == 'filled':  # 已成交, 全部成交
                        # ----------------------------------------- 调接口查询hb现货成交明细 -----------------------------------------
                        deal_api_result = huobi_spot.get_order_deal_info(order_id)
                        deal_status = deal_api_result.get('status', '')
                        deal_datas = deal_api_result.get('data', [])
                        if deal_status.lower() == 'ok' and deal_datas:
                            deal_data = deal_datas[0]
                            deal_price = deal_data.get('price', '')  # 成交价格
                            if deal_price: deal_price = self.exchanges.fill_num(deal_price, price_precision)
                            # ----------------------------------------- 调接口查询hb现货成交明细 -----------------------------------------

                            last_arbitrage_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            deal_time = data.get('finished-at', '')  # 终结态时间(监听的已成交) 1494901400468
                            if deal_time: deal_time = self.exchanges.timestamp_to_date(str(deal_time)[: 10])

                            delivery_id, stock_id = self.exchanges.get_user_id(account_id)
                            _logger.info(
                                '----- monitor_hb_spot_order_status, account_id: {}, delivery_id: {}, stock_id: {} -----'.format(
                                    account_id, delivery_id, stock_id))

                            recipients = self.exchanges.send_detail_email.get_account_emails(delivery_id)  # 每次发邮件前更新邮箱
                            search_dict = get_table_data(strategy_mysql_conn, const.T_FUTURES_SPOT_ENTRUST,
                                                         ['entrust_time'],
                                                         ['order_id'], [order_id])  # 取委托时间
                            if search_dict:
                                entrust_time = search_dict.get('entrust_time', '')
                                entrust_time = entrust_time.strftime('%Y-%m-%d %H:%M:%S')

                            # ------------------ 更新买入现货数量, 买入现货成本
                            if _type == 'buy':
                                buy_spot_amount = float(strategy_params.get('buy_spot_amount', 0)) + float(deal_amount)
                                strategy_params['buy_spot_amount'] = buy_spot_amount  # 更新买入现货数量
                                strategy_params['spot_purchase_cost'] = deal_price  # 更新买入现货成本(成交均价)
                                _logger.info(
                                    '------ monitor_hb_spot_order_status, buy, 更新买入现货数量, buy_spot_amount: {} -----'.format(
                                        buy_spot_amount))

                            elif _type == 'sell':
                                buy_spot_amount = float(strategy_params.get('buy_spot_amount', 0)) - float(deal_amount)
                                strategy_params['buy_spot_amount'] = buy_spot_amount  # 更新买入现货数量
                                _logger.info(
                                    '------ monitor_hb_spot_order_status, sell, 更新买入现货数量, buy_spot_amount: {} -----'.format(
                                        buy_spot_amount))

                            self.exchanges.update_t_futures_spot_postion(strategy_params, last_arbitrage_time)  # 更新持仓表
                            self.exchanges.save_t_futures_spot_deal(strategy_params, order_id, deal_price, deal_amount,
                                                                    deal_fee, entrust_type, entrust_time,
                                                                    deal_time)  # 存成交表

                            # -------------------------------- 推送现货成交日志 ---------------------------------------------
                            strategy_params['information'] = '{}，{}，委托价格：{}，委托数量：{}，成交价格：{}，成交数量：{}，手续费：{}'.format(
                                order_trade_type,
                                currency,
                                trade_price,
                                trade_amount,
                                deal_price,
                                deal_amount,
                                deal_fee)
                            strategy_params['log_type'] = 'deal'
                            self.exchanges.push_log(strategy_params, 'log')
                            # -------------------------------- 推送现货成交日志 ---------------------------------------------

                            # ----------------------------- 发送全部成交邮件 ---------------------------------------
                            email_detail = '账户名称：{}\r\n现货名称：{}\r\n交易类型：{}\r\n委托价格：{}\r\n成交价格：{}\r\n委托数量：{}\r\n成交数量：{}\r\n委托时间：{}\r\n成交时间：{}\r\n手续费：{}\r\n订单编号：{}'.format(
                                account_name,
                                currency.upper(),
                                order_trade_type,
                                trade_price,
                                deal_price,
                                trade_amount,
                                deal_amount,
                                entrust_time,
                                last_arbitrage_time,
                                deal_fee,
                                order_id)

                            email_detail = self.exchanges.handler_email_content(email_detail)
                            self.exchanges.send_grid_trade_email(email_detail, recipients, '期现策略：您有新的成交记录')
                            _logger.info(
                                '----- 发送期现交易成交邮件成功, recipients: {}, email_detail: {} -----'.format(recipients,
                                                                                                    email_detail))
                            # ----------------------------- 发送全部成交邮件 ---------------------------------------

                            deal_result = True  # 全部成交, 更新
                            break

                    elif state.lower() == 'partial-filled':  # 部分成交
                        # ----------------------------------------- 调接口查询hb现货成交明细 -----------------------------------------
                        deal_api_result = huobi_spot.get_order_deal_info(order_id)
                        deal_status = deal_api_result.get('status', '')
                        deal_datas = deal_api_result.get('data', [])
                        if deal_status.lower() == 'ok' and deal_datas:
                            deal_data = deal_datas[0]
                            deal_price = deal_data.get('price', '')  # 成交价格
                            if deal_price: deal_price = self.exchanges.fill_num(deal_price, price_precision)
                            # ----------------------------------------- 调接口查询hb现货成交明细 -----------------------------------------

                            strategy_params['part_deal_amount'] = deal_amount  # 部分成交数量
                            strategy_params['part_deal_price'] = deal_price  # 部分成交价格
                            strategy_params['order_id'] = order_id

                            self.exchanges.update_t_futures_spot_entrust(strategy_params)  # 更新委托成交数量

                    elif state.lower() == 'canceled':  # 已撤单
                        _logger.info(
                            '----- monitor_hb_spot_order_status, 监听到已撤单， order_id: {}, order_status: {} -----'.format(
                                order_id, state))

                        # 删除委托
                        delete_result = self.exchanges.delete_entrust_data(order_id)
                        if delete_result:
                            # 推送已撤单日志
                            strategy_params['information'] = '{}，委托价格：{}，委托数量：{}，订单编号：{}'.format(
                                order_trade_type,
                                trade_price,
                                trade_amount,
                                order_id)
                            strategy_params['log_type'] = 'cancel'
                            self.exchanges.push_log(strategy_params, 'log')

                        break  # 退出循环

                    futures_spot_order_query_interval = self.exchanges.get_strategy_param().get(
                        'futures_spot_order_query_interval', 1)
                    time.sleep(int(futures_spot_order_query_interval))  # 每隔一段时间调接口查是否全部成交
        except Exception as e:
            _logger.error('----- monitor_hb_spot_order_status, {} -----'.format(traceback.format_exc()))
        finally:
            return deal_result

    def ok_futures_place_order(self, strategy_params, api_config_params):
        ''' ok合约下单 '''
        place_result, order_id = False, ''
        try:
            _logger.info(
                '----- ok_futures_place_order, OK正在合约下单, strategy_params: {}, api_config_params: {} -----'.format(
                    strategy_params, api_config_params))

            strategy_params = copy.copy(strategy_params)
            name = strategy_params.get('name', '')
            currency = strategy_params.get('currency', '')
            trade_price = strategy_params.get('trade_price')  # 委托价格
            trade_amount = strategy_params.get('trade_amount')  # 委托数量

            access_key = api_config_params.get('access_key', '')
            secret_key = api_config_params.get('secret_key', '')
            pass_phrase = api_config_params.get('pass_phrase', '')
            instrument_id = self.exchanges.get_instrument_id(strategy_params)  # BTC-USD-180213

            _place_type = ''
            _order_type = ''
            log_trade_type = ''
            _match_price = '0'  # 默认为0, 非对手价

            type = strategy_params.get('type', '')

            if type == 'buy':  # 开仓单
                buy_type = strategy_params.get('buy_type', '')
                _place_type = '2'  # 卖出开空
                log_trade_type = '卖出开空'

                if buy_type == '最新价':  # 限价下单
                    _order_type = '0'
                elif buy_type == '市价':  # 市价下单
                    _order_type = '4'
                elif buy_type == '对手价':  # 对手价下单
                    _order_type = '0'
                    _match_price = '1'
            elif type == 'sell':  # 平仓单
                sell_type = strategy_params.get('sell_type', '')
                _place_type = '4'  # 买入平空
                log_trade_type = '买入平空'

                if sell_type == '最新价':  # 限价下单
                    _order_type = '0'
                elif sell_type == '市价':  # 市价下单
                    _order_type = '4'
                elif sell_type == '对手价':  # 对手价下单
                    _order_type = '0'
                    _match_price = '1'

            # ---------------------------------------- 下单部分 ------------------------------------------------------
            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_futures = OkexFutures(req)

            _logger.info(
                '----- ok_futures_place_order, api请求参数, access_key: {}, secret_key: {}, pass_phrase: {}, instrument_id: {}, '
                'type: {}, price: {}, '
                'size: {}, client_oid: {}, '
                'order_type: {}, match_price: {} -----'.format(access_key, secret_key, pass_phrase,
                                                               instrument_id, _place_type, trade_price, trade_amount,
                                                               '', _order_type, _match_price))

            api_result = okex_futures.place_order(instrument_id=instrument_id,
                                                  type=str(_place_type),
                                                  price=str(trade_price),
                                                  size=str(trade_amount),
                                                  client_oid='',
                                                  order_type=_order_type,
                                                  match_price=_match_price)

            _logger.info('----- ok_futures_place_order, api返回结果: {} -----'.format(api_result))

            error_code = api_result.get('error_code', '')
            error_message = api_result.get('error_message', '')

            trade_price = self.exchanges.handler_entrust_price(trade_price)  # 更新委托价格

            # 下单成功, 存委托表
            if str(error_code) == '0':
                order_id = api_result.get('order_id')

                strategy_params['order_id'] = order_id
                update_result = self.exchanges.update_t_futures_spot_entrust(strategy_params)  # 存委托表

                if not update_result:
                    _logger.error('----- ok_futures_place_order, 保存委托表失败 -----')
                    return

                # -------------------------------- 推送合约下单日志 ---------------------------------------------
                strategy_params['information'] = '{}，{}，委托价格：{}，委托数量：{}'.format(log_trade_type,
                                                                                '-'.join(name.split('-')[: -1]),
                                                                                trade_price, trade_amount)
                strategy_params['log_type'] = 'place'
                self.exchanges.push_log(strategy_params, 'log')

                place_result = True
                # -------------------------------- 推送合约下单日志 ---------------------------------------------

            else:  # 下单失败
                # -------------------------------- 推送合约下单失败日志 ---------------------------------------------
                information = '合约下单失败，{}。[委托价格：{}，委托数量：{}]'.format(error_message, trade_price, trade_amount)
                strategy_params['information'] = information
                strategy_params['log_type'] = 'error'
                self.exchanges.push_log(strategy_params, 'log')
                # -------------------------------- 推送合约下单失败日志 ---------------------------------------------

                # ------------------------------- 存下单失败操作日志 --------------------------------------------
                self.exchanges.futures_spot_save_operator_log(strategy_params, '失败')
                # ------------------------------- 存下单失败操作日志 --------------------------------------------
            # ---------------------------------------- 下单部分 ------------------------------------------------------

        except Exception as e:
            _logger.error('----- ok_futures_place_order, {} -----'.format(traceback.format_exc()))
        finally:
            return place_result, order_id

    def hb_futures_place_order(self, strategy_params, api_config_params):
        ''' hb合约下单 '''
        place_result, order_id, whether_market = False, '', False
        try:
            _logger.info(
                '----- hb_futures_place_order, HB正在合约下单, strategy_params: {}, api_config_params: {} -----'.format(
                    strategy_params, api_config_params))

            strategy_params = copy.copy(strategy_params)
            account_id = strategy_params.get('account_id', '')
            currency = strategy_params.get('currency', '')
            name = strategy_params.get('name', '')
            trade_price = strategy_params.get('trade_price')  # 委托价格
            trade_amount = strategy_params.get('trade_amount')  # 委托数量
            contract_type = strategy_params.get('contract_type')  # 合约类型
            instrument_id = self.exchanges.get_instrument_id(strategy_params)  # BTC
            pair_id = self.exchanges.get_pair_id(strategy_params)
            contract_time = self.exchanges.get_contract_date(pair_id)
            contract_code = '{}{}'.format(currency, contract_time)

            URL = api_config_params.get('futures_url', '')  # 合约URL
            ACCESS_KEY = api_config_params.get('access_key', '')
            SECRET_KEY = api_config_params.get('secret_key', '')

            _type = ''
            _direction = ''
            _offset = ''
            log_trade_type = ''
            order_price_type = ''

            type = strategy_params.get('type', '')

            if type == 'buy':  # 开仓单 卖出开空
                buy_type = strategy_params.get('buy_type', '')
                _direction = 'sell'
                _offset = 'open'
                log_trade_type = '卖出开空'

                if buy_type == '最新价':  # 限价下单
                    _type = 'limit'
                    order_price_type = 'limit'
                elif buy_type == '市价':  # 市价下单
                    _type = 'market'
                    order_price_type = 'optimal_5'
                elif buy_type == '对手价':  # 对手价下单
                    _type = 'rival'
                    order_price_type = 'opponent'

            elif type == 'sell':  # 平仓单 买入平空
                sell_type = strategy_params.get('sell_type', '')
                _direction = 'buy'
                _offset = 'close'
                log_trade_type = '买入平空'

                if sell_type == '最新价':  # 限价下单
                    _type = 'limit'
                    order_price_type = 'limit'
                elif sell_type == '市价':  # 市价下单
                    _type = 'market'
                    order_price_type = 'optimal_5'
                elif sell_type == '对手价':
                    _type = 'rival'
                    order_price_type = 'opponent'

            # ---------------------------------------- 下单部分 ------------------------------------------------------
            huobi_futures = HuobiFutures(URL, ACCESS_KEY, SECRET_KEY)
            _logger.info(
                '----- hb_futures_place_order, api请求参数, symbol: {},  contract_type: {}, contract_code: {}, price: {}, volume: {}, direction: {}, offset: {}, order_price_type: {} -----'.format(
                    instrument_id, contract_type, contract_code, trade_price, trade_amount, _direction, _offset,
                    order_price_type))

            if trade_price in ['对手价']:
                api_result = huobi_futures.send_contract_order(symbol=instrument_id,
                                                               contract_type=contract_type,
                                                               contract_code=contract_code.lower(),
                                                               client_order_id='',
                                                               volume=int(trade_amount),
                                                               direction=_direction,
                                                               offset=_offset,
                                                               lever_rate=int(1),
                                                               order_price_type=order_price_type
                                                               )
            else:
                api_result = huobi_futures.send_contract_order(symbol=instrument_id,
                                                               contract_type=contract_type,
                                                               contract_code=contract_code.lower(),
                                                               client_order_id='',
                                                               price=str(trade_price),
                                                               volume=int(trade_amount),
                                                               direction=_direction,
                                                               offset=_offset,
                                                               lever_rate=int(1),
                                                               order_price_type=order_price_type
                                                               )
            status = api_result.get('status', '')
            data = api_result.get('data', {})

            _logger.info('----- hb_futures_place_order, api返回结果: {} -----'.format(api_result))

            trade_price = self.exchanges.handler_entrust_price(trade_price)  # 更新委托价格

            # 下单成功, 存委托表
            if status.lower() == 'ok' and data:
                order_id = str(data.get('order_id_str', ''))

                strategy_params['order_id'] = order_id
                update_result = self.exchanges.update_t_futures_spot_entrust(strategy_params)  # 存委托表

                if not update_result:
                    _logger.error('----- hb_futures_place_order, 保存委托表失败 -----')
                    return

                # -------------------------------- 推送合约下单日志 ---------------------------------------------
                strategy_params['information'] = '{}，{}，委托价格：{}，委托数量：{}'.format(log_trade_type,
                                                                                '-'.join(name.split('-')[: -1]),
                                                                                trade_price, trade_amount)
                strategy_params['log_type'] = 'place'
                self.exchanges.push_log(strategy_params, 'log')
                # -------------------------------- 推送合约下单日志 ---------------------------------------------

                place_result = True
            else:  # 下单失败
                # -------------------------------- 推送合约下单失败日志 ---------------------------------------------
                error_message = api_result.get('err_msg', '')
                information = '合约下单失败，{}。[委托价格：{}，委托数量：{}]'.format(error_message, trade_price, trade_amount)
                strategy_params['information'] = information
                strategy_params['log_type'] = 'error'
                self.exchanges.push_log(strategy_params, 'log')
                # -------------------------------- 推送合约下单失败日志 ---------------------------------------------

                # ------------------------------- 存下单失败操作日志 --------------------------------------------
                self.exchanges.futures_spot_save_operator_log(strategy_params, '失败')
                # ------------------------------- 存下单失败操作日志 --------------------------------------------
            # ---------------------------------------- 下单部分 ------------------------------------------------------
        except Exception as e:
            _logger.error('----- hb_futures_place_order, {} -----'.format(traceback.format_exc()))
        finally:
            return place_result, order_id

    def monitor_ok_futures_order_status(self, strategy_params, api_config_params, order_id, symbol):
        deal_result = False  # 成交结果
        try:
            _logger.info(
                '----- monitor_ok_futures_order_status, strategy_params: {}, api_config_params: {}, order_id: {} '
                '-----'.format(strategy_params, api_config_params, order_id))

            price_precision = self.symbols_dict['OK']['futures'].get(symbol, {}).get('price_precision', '0')
            amount_precision = self.symbols_dict['OK']['futures'].get(symbol, {}).get('amount_precision', '0')

            strategy_params = copy.copy(strategy_params)
            currency = strategy_params.get('currency', '')
            trade_price = strategy_params.get('trade_price', 0)  # 委托价格
            trade_amount = strategy_params.get('trade_amount', 0)  # 委托数量

            access_key = api_config_params.get('access_key', '')
            secret_key = api_config_params.get('secret_key', '')
            pass_phrase = api_config_params.get('pass_phrase', '')

            # instrument_id = '{}-{}'.format(currency.upper(), 'USD')
            instrument_id = self.exchanges.get_instrument_id(strategy_params)
            _logger.info('----- monitor_ok_futures_order_status, api请求参数, api_key: {}, api_seceret_key: {}, '
                         'passphrase: {}, order_id: {} -----'.format(access_key, secret_key, pass_phrase, order_id))
            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_futures = OkexFutures(req)

            while True:
                api_result = okex_futures.get_order_detail(instrument_id=instrument_id, order_id=order_id)
                _logger.info('----- monitor_ok_futures_order_status, api_result: {} -----'.format(api_result))
                state = api_result.get('state', '')

                # ------------------------ 获取订单信息 ---------------------------------------
                entrust_time = ''
                order_trade_type = ''

                name = strategy_params.get('name', '')
                entrust_type = strategy_params.get('entrust_type', '')
                _type = strategy_params.get('type', '')
                account_name = strategy_params.get('account_name')
                account_id = strategy_params.get('account_id')

                timestamp = api_result.get('timestamp', '')  # 订单创建时间
                deal_amount = api_result.get('filled_qty', '')  # 成交数量
                if deal_amount: deal_amount = self.exchanges.fill_num(deal_amount, amount_precision)
                deal_price = api_result.get('price_avg', '')  # 成交价格
                if deal_price: deal_price = self.exchanges.fill_num(deal_price, price_precision)
                deal_fee = api_result.get('fee', '')  # 成交手续费
                if deal_fee: deal_fee = self.exchanges.fill_num(-abs(float(deal_fee)), 6)
                # ------------------------ 获取订单信息 ---------------------------------------

                trade_price = self.exchanges.handler_entrust_price(trade_price)  # 更新委托价格
                order_trade_type = '卖出开空' if _type == 'buy' else '买入平空'

                if str(state) == '2':  # 已成交, 全部成交
                    last_arbitrage_time = deal_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                    delivery_id, stock_id = self.exchanges.get_user_id(account_id)
                    search_dict = get_table_data(strategy_mysql_conn, const.T_FUTURES_SPOT_ENTRUST, ['entrust_time'],
                                                 ['order_id'], [order_id])  # 取委托时间
                    if search_dict:
                        entrust_time = search_dict.get('entrust_time', '')
                        entrust_time = entrust_time.strftime('%Y-%m-%d %H:%M:%S')

                    recipients = self.exchanges.send_detail_email.get_account_emails(delivery_id)  # 每次发邮件前更新邮箱

                    if _type == 'buy':
                        contract_positions_amount = float(strategy_params.get('contract_positions_amount', 0)) + float(
                            deal_amount)
                        strategy_params['contract_positions_amount'] = contract_positions_amount  # 更新合约持仓数量
                        strategy_params['contract_opening_cost'] = deal_price  # 更新合约开仓成本
                        _logger.info(
                            '------ monitor_ok_futures_order_status, buy, 更新合约持仓数量, contract_positions_amount: {} -----'.format(
                                contract_positions_amount))
                    elif _type == 'sell':
                        contract_positions_amount = float(strategy_params.get('contract_positions_amount', 0)) - float(
                            deal_amount)
                        strategy_params['contract_positions_amount'] = contract_positions_amount  # 更新合约持仓数量
                        _logger.info(
                            '------ monitor_ok_futures_order_status, sell, 更新合约持仓数量, contract_positions_amount: {} -----'.format(
                                contract_positions_amount))

                    self.exchanges.update_t_futures_spot_postion(strategy_params, last_arbitrage_time)  # 更新持仓表
                    self.exchanges.save_t_futures_spot_deal(strategy_params, order_id, deal_price, deal_amount,
                                                            deal_fee, entrust_type, entrust_time, deal_time)  # 存成交表

                    # -------------------------------- 推送合约成交日志 ---------------------------------------------
                    strategy_params['information'] = '{}，{}，委托价格：{}，委托数量：{}，成交价格：{}，成交数量：{}，手续费：{}'.format(
                        order_trade_type,
                        '-'.join(name.split('-')[: -1]),
                        trade_price,
                        trade_amount,
                        deal_price,
                        deal_amount,
                        deal_fee)
                    strategy_params['log_type'] = 'deal'
                    self.exchanges.push_log(strategy_params, 'log')
                    # -------------------------------- 推送合约成交日志 ---------------------------------------------

                    # ----------------------------- 发送全部成交邮件 ---------------------------------------
                    email_detail = '账户名称：{}\r\n合约类型：{}\r\n交易类型：{}\r\n委托价格：{}\r\n成交价格：{}\r\n委托数量：{}\r\n成交数量：{}\r\n委托时间：{}\r\n成交时间：{}\r\n手续费：{}\r\n订单编号：{}'.format(
                        account_name,
                        '-'.join(str(name).split('-')[: -1]),
                        order_trade_type,
                        trade_price,
                        deal_price,
                        trade_amount,
                        deal_amount,
                        entrust_time,
                        last_arbitrage_time,
                        deal_fee,
                        order_id)

                    email_detail = self.exchanges.handler_email_content(email_detail)
                    self.exchanges.send_grid_trade_email(email_detail, recipients, '期现策略：您有新的成交记录')
                    _logger.info(
                        '----- 发送期现交易成交邮件成功, recipients: {}, email_detail: {} -----'.format(recipients, email_detail))
                    # ----------------------------- 发送全部成交邮件 ---------------------------------------

                    deal_result = True  # 全部成交, 更新
                    break

                elif str(state) == '1':  # 部分成交
                    strategy_params['part_deal_amount'] = deal_amount  # 部分成交数量
                    strategy_params['part_deal_price'] = deal_price  # 部分成交价格
                    strategy_params['order_id'] = order_id

                    self.exchanges.update_t_futures_spot_entrust(strategy_params)  # 更新委托成交数量

                elif str(state) == '-1':  # 撤单成功
                    _logger.info(
                        '----- monitor_ok_futures_order_status, 监听到已撤单， order_id: {}, order_status: {} -----'.format(
                            order_id, state))

                    # 删除委托
                    delete_result = self.exchanges.delete_entrust_data(order_id)
                    if delete_result:
                        # 推送已撤单日志
                        strategy_params['information'] = '{}，委托价格：{}，委托数量：{}，订单编号：{}'.format(
                            order_trade_type,
                            trade_price,
                            trade_amount,
                            order_id)
                        strategy_params['log_type'] = 'cancel'
                        self.exchanges.push_log(strategy_params, 'log')

                    break  # 退出循环

                elif str(state) == '-2':  # 已失败
                    _logger.info(
                        '----- monitor_ok_futures_order_status, 监听到已失败， order_id: {}, order_status: {} -----'.format(
                            order_id, state))

                    # 删除委托
                    delete_result = self.exchanges.delete_entrust_data(order_id)
                    if delete_result:
                        # 推送已失败日志
                        strategy_params['information'] = '{}订单失败，委托价格：{}，委托数量：{}，订单编号：{}'.format(
                            order_trade_type,
                            trade_price,
                            trade_amount,
                            order_id)
                        strategy_params['log_type'] = 'info'
                        self.exchanges.push_log(strategy_params, 'log')

                    break  # 退出循环

                # 轮询间隔时间
                futures_spot_order_query_interval = self.exchanges.get_strategy_param().get(
                    'futures_spot_order_query_interval', 1)
                time.sleep(int(futures_spot_order_query_interval))  # 每隔一段时间调接口查是否全部成交
        except Exception as e:
            _logger.error('----- monitor_ok_futures_order_status, {} -----'.format(traceback.format_exc()))
        finally:
            return deal_result

    def monitor_hb_futures_order_status(self, strategy_params, api_config_params, order_id, symbol):
        deal_result = False  # 成交结果
        try:
            currency = strategy_params.get('currency', '')
            _logger.info(
                '----- monitor_hb_futures_order_status, strategy_params: {}, api_config_params: {}, order_id: {} '
                '-----'.format(strategy_params, api_config_params, order_id))

            price_precision = self.symbols_dict['HB']['futures'].get(symbol, {}).get('price_precision', '0')
            amount_precision = self.symbols_dict['HB']['futures'].get(symbol, {}).get('amount_precision', '0')

            strategy_params = copy.copy(strategy_params)
            trade_price = strategy_params.get('trade_price', 0)  # 委托价格
            trade_amount = strategy_params.get('trade_amount', 0)  # 委托数量

            URL = api_config_params.get('futures_url', '')  # 合约URL
            ACCESS_KEY = api_config_params.get('access_key', '')
            SECRET_KEY = api_config_params.get('secret_key', '')

            _logger.info('----- monitor_hb_futures_order_status, api请求参数, URL: {}, ACCESS_KEY: {}, '
                         'SECRET_KEY: {}, order_id: {} -----'.format(URL, ACCESS_KEY, SECRET_KEY, order_id))
            huobi_futures = HuobiFutures(URL, ACCESS_KEY, SECRET_KEY)

            while True:
                api_result = huobi_futures.get_contract_order_info(symbol=currency, order_id=order_id)
                _logger.info('----- monitor_hb_futures_order_status, api_result: {} -----'.format(api_result))
                status = api_result.get('status', '')
                data = api_result.get('data', [])

                if status.lower() == 'ok' and data:
                    data_dict = data[0]
                    order_status = data_dict.get('status', '')

                    # ------------------- 获取订单信息 ------------------------------
                    entrust_time = ''
                    order_trade_type = ''
                    name = strategy_params.get('name', '')
                    entrust_type = strategy_params.get('entrust_type', '')
                    _type = strategy_params.get('type', '')
                    account_name = strategy_params.get('account_name')
                    account_id = strategy_params.get('account_id')

                    create_at = data_dict.get('created_at', '')  # 订单创建时间
                    deal_amount = data_dict.get('trade_volume', '')  # 成交数量
                    if deal_amount: deal_amount = self.exchanges.fill_num(deal_amount, amount_precision)
                    deal_price = data_dict.get('trade_avg_price', '')  # 成交价格
                    if deal_price: deal_price = self.exchanges.fill_num(deal_price, price_precision)
                    deal_fee = data_dict.get('fee', '')  # 成交手续费
                    if deal_fee: deal_fee = self.exchanges.fill_num(-abs(float(deal_fee)), 6)
                    # ------------------- 获取订单信息 ------------------------------

                    trade_price = self.exchanges.handler_entrust_price(trade_price)  # 更新委托价格
                    order_trade_type = '卖出开空' if _type == 'buy' else '买入平空'

                    if str(order_status) == '6':  # 已成交, 全部成交
                        last_arbitrage_time = deal_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                        delivery_id, stock_id = self.exchanges.get_user_id(account_id)
                        search_dict = get_table_data(strategy_mysql_conn, const.T_FUTURES_SPOT_ENTRUST,
                                                     ['entrust_time'],
                                                     ['order_id'], [order_id])  # 取委托时间
                        if search_dict:
                            entrust_time = search_dict.get('entrust_time', '')
                            entrust_time = entrust_time.strftime('%Y-%m-%d %H:%M:%S')

                        recipients = self.exchanges.send_detail_email.get_account_emails(delivery_id)  # 每次发邮件前更新邮箱

                        if _type == 'buy':
                            contract_positions_amount = float(
                                strategy_params.get('contract_positions_amount', 0)) + float(
                                deal_amount)
                            strategy_params['contract_positions_amount'] = contract_positions_amount  # 更新合约持仓数量
                            strategy_params['contract_opening_cost'] = deal_price  # 更新合约开仓成本
                            _logger.info(
                                '------ monitor_hb_futures_order_status, buy, 更新合约持仓数量, contract_positions_amount: {} -----'.format(
                                    contract_positions_amount))
                        elif _type == 'sell':
                            contract_positions_amount = float(
                                strategy_params.get('contract_positions_amount', 0)) - float(
                                deal_amount)
                            strategy_params['contract_positions_amount'] = contract_positions_amount  # 更新合约持仓数量
                            _logger.info(
                                '------ monitor_hb_futures_order_status, sell, 更新合约持仓数量, contract_positions_amount: {} -----'.format(
                                    contract_positions_amount))

                        self.exchanges.update_t_futures_spot_postion(strategy_params, last_arbitrage_time)  # 更新持仓表
                        self.exchanges.save_t_futures_spot_deal(strategy_params, order_id, deal_price, deal_amount,
                                                                deal_fee, entrust_type, entrust_time, deal_time)  # 存成交表

                        # -------------------------------- 推送合约成交日志 ---------------------------------------------
                        strategy_params['information'] = '{}，{}，委托价格：{}，委托数量：{}，成交价格：{}，成交数量：{}，手续费：{}'.format(
                            order_trade_type,
                            '-'.join(name.split('-')[: -1]),
                            trade_price,
                            trade_amount,
                            deal_price,
                            deal_amount,
                            deal_fee)
                        strategy_params['log_type'] = 'deal'
                        self.exchanges.push_log(strategy_params, 'log')
                        # -------------------------------- 推送合约成交日志 ---------------------------------------------

                        # ----------------------------- 发送全部成交邮件 ---------------------------------------
                        email_detail = '账户名称：{}\r\n合约类型：{}\r\n交易类型：{}\r\n委托价格：{}\r\n成交价格：{}\r\n委托数量：{}\r\n成交数量：{}\r\n委托时间：{}\r\n成交时间：{}\r\n手续费：{}\r\n订单编号：{}'.format(
                            account_name,
                            '-'.join(str(name).split('-')[: -1]),
                            order_trade_type,
                            trade_price,
                            deal_price,
                            trade_amount,
                            deal_amount,
                            entrust_time,
                            last_arbitrage_time,
                            deal_fee,
                            order_id)

                        email_detail = self.exchanges.handler_email_content(email_detail)
                        self.exchanges.send_grid_trade_email(email_detail, recipients, '期现策略：您有新的成交记录')
                        _logger.info(
                            '----- 发送期现交易成交邮件成功, recipients: {}, email_detail: {} -----'.format(recipients,
                                                                                                email_detail))
                        # ----------------------------- 发送全部成交邮件 ---------------------------------------

                        deal_result = True  # 全部成交, 更新
                        break

                    elif str(order_status) == '4':  # 部分成交
                        strategy_params['part_deal_amount'] = deal_amount  # 部分成交数量
                        strategy_params['part_deal_price'] = deal_price  # 部分成交价格
                        strategy_params['order_id'] = order_id

                        self.exchanges.update_t_futures_spot_entrust(strategy_params)  # 更新委托成交数量

                    elif str(order_status) == '7':  # 已撤单
                        _logger.info(
                            '----- monitor_hb_futures_order_status, 监听到已撤单， order_id: {}, order_status: {} -----'.format(
                                order_id, order_status))
                        # 删除委托
                        delete_result = self.exchanges.delete_entrust_data(order_id)
                        if delete_result:
                            # 推送已撤单日志
                            strategy_params['information'] = '{}，委托价格：{}，委托数量：{}，订单编号：{}'.format(
                                order_trade_type,
                                trade_price,
                                trade_amount,
                                order_id)
                            strategy_params['log_type'] = 'cancel'
                            self.exchanges.push_log(strategy_params, 'log')

                        break  # 退出循环

                    futures_spot_order_query_interval = self.exchanges.get_strategy_param().get(
                        'futures_spot_order_query_interval', 1)
                    time.sleep(int(futures_spot_order_query_interval))  # 每隔一段时间调接口查是否全部成交
        except Exception as e:
            _logger.error('----- monitor_hb_futures_order_status, {} -----'.format(traceback.format_exc()))
        finally:
            return deal_result

    def ok_transfer(self, strategy_params, api_config_params, _from, _to, _amount):
        ''' ok资金划转 '''
        transfer_result = False  # 划转结果
        try:
            currency = strategy_params.get('currency', '').lower()

            access_key = api_config_params.get('access_key', '')
            secret_key = api_config_params.get('secret_key', '')
            pass_phrase = api_config_params.get('pass_phrase', '')

            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_account = OkexAccount(req)

            # ------------------------ _amount 调接口查余额 ------------------------------------------------
            c_strategy_params = copy.copy(strategy_params)

            if strategy_params.get('type') == 'buy':
                _amount = self.exchanges.get_ok_spot_balance(c_strategy_params)
            elif strategy_params.get('type') == 'sell':
                _amount = self.exchanges.get_ok_futures_balance(c_strategy_params)

            _amount = self.exchanges.fill_num(_amount, '8')  # 默认8位
            # ------------------------ _amount 调接口查余额 ------------------------------------------------

            if _from in ['3', '5', '9']:
                _logger.info('----- ok_transfer, api请求参数, api_key: {}, api_seceret_key: {}, '
                             'passphrase: {}, currency: {}, amount: {}, _from: {}, _to: {}  -----'.format(
                    access_key,
                    secret_key,
                    pass_phrase,
                    currency, _amount,
                    _from, _to))

                api_result = okex_account.transfer(currency=currency, amount=_amount, _from=_from, _to=_to)

            elif _to in ['3', '5', '9']:
                instrument_id = '{}-{}'.format(currency, 'usd')
                to_instrument_id = instrument_id.lower()

                _logger.info('----- ok_transfer, api请求参数, api_key: {}, api_seceret_key: {}, '
                             'passphrase: {}, currency: {}, amount: {}, _from: {}, _to: {}, to_instrument_id: {}  -----'.format(
                    access_key,
                    secret_key,
                    pass_phrase,
                    currency, _amount,
                    _from, _to, to_instrument_id))
                api_result = okex_account.transfer(currency=currency, amount=_amount, _from=_from, _to=_to,
                                                   to_instrument_id=to_instrument_id)

            else:
                _logger.info('----- ok_transfer, api请求参数, api_key: {}, api_seceret_key: {}, '
                             'passphrase: {}, currency: {}, amount: {}, _from: {}, _to: {}  -----'.format(
                    access_key,
                    secret_key,
                    pass_phrase,
                    currency, _amount,
                    _from, _to, ))
                api_result = okex_account.transfer(currency=currency, amount=_amount, _from=_from, _to=_to)

            _logger.info('----- ok_transfer, api返回结果: {} -----'.format(api_result))
            result = api_result.get('result', '')
            if result == True:
                # -------------------------------- 推送划转日志 ---------------------------------------------
                if strategy_params.get('type') == 'buy':
                    strategy_params['information'] = '从币币账户转入合约账户，数量：{}{}'.format(_amount, currency.upper())
                elif strategy_params.get('type') == 'sell':
                    strategy_params['information'] = '从合约账户转入币币账户，数量：{}{}'.format(_amount, currency.upper())

                strategy_params['log_type'] = 'transfer'
                self.exchanges.push_log(strategy_params, 'log')
                # -------------------------------- 推送划转日志 ---------------------------------------------

                transfer_result = True
            else:  # 划转失败
                # -------------------------------- 推送划转失败日志 ---------------------------------------------
                information = '划转失败，err_code：{}，err_msg：{}, [划转数量：{}{}]'.format(
                    api_result.get('error_code', ''),
                    api_result.get('error_message', ''),
                    _amount,
                    currency.upper())

                strategy_params['information'] = information
                strategy_params['log_type'] = 'transfer'
                self.exchanges.push_log(strategy_params, 'log')
                _logger.info('----- ok划转失败, information: {} -----'.format(information))
                # -------------------------------- 推送划转失败日志 ---------------------------------------------
        except Exception as e:
            _logger.error('----- ok_transfer, {} -----'.format(traceback.format_exc()))
        finally:
            return transfer_result

    def hb_transfer(self, strategy_params, api_config_params, _transfer_type, _amount):
        ''' hb资金划转 '''
        transfer_result = False  # 划转结果
        try:
            _logger.info(
                '---- hb_transfer, strategy_params: {}, _transfer_type: {}, _amount: {} ----'.format(strategy_params,
                                                                                                     _transfer_type,
                                                                                                     _amount))
            currency = strategy_params.get('currency', '')

            URL = api_config_params.get('spot_url', '')  # 现货URL
            ACCESS_KEY = api_config_params.get('access_key', '')
            SECRET_KEY = api_config_params.get('secret_key', '')

            # ------------------------ _amount 调接口查余额 ------------------------------------------------
            c_strategy_params = copy.copy(strategy_params)

            if strategy_params.get('type') == 'buy':
                _amount = self.exchanges.get_hb_spot_balance(c_strategy_params)
            elif strategy_params.get('type') == 'sell':
                _amount = self.exchanges.get_hb_futures_balance(c_strategy_params)

            _amount = self.exchanges.fill_num(_amount, '8')
            # ------------------------ _amount 调接口查余额 ------------------------------------------------

            _logger.info('----- hb_transfer, api请求参数, URL: {}, ACCESS_KEY: {}, '
                         'SECRET_KEY: {}, amount: {}, type: {} -----'.format(URL, ACCESS_KEY, SECRET_KEY, _amount,
                                                                             _transfer_type))

            huobi_account = HuobiAccount(URL, ACCESS_KEY, SECRET_KEY)
            api_result = huobi_account.transfer(currency=currency, amount=_amount,
                                                type=_transfer_type)  # futures-to-pro, pro-to-futures

            _logger.info('----- hb_transfer, api返回结果: {} -----'.format(api_result))
            status = api_result.get('status', '')
            if str(status).lower() == 'ok':  # 划转成功
                # -------------------------------- 推送划转日志 ---------------------------------------------
                if strategy_params.get('type') == 'buy':
                    strategy_params['information'] = '从币币账户转入合约账户，数量：{}{}'.format(_amount, currency.upper())
                elif strategy_params.get('type') == 'sell':
                    strategy_params['information'] = '从合约账户转入币币账户，数量：{}{}'.format(_amount, currency.upper())

                strategy_params['log_type'] = 'transfer'
                self.exchanges.push_log(strategy_params, 'log')
                # -------------------------------- 推送划转日志 ---------------------------------------------

                transfer_result = True
            else:
                err_code = api_result.get('err-code')
                err_msg = api_result.get('err-msg')
                _logger.info('----- hb_transfer, 划转失败, err_code: {}, err_msg: {} -----'.format(err_code, err_msg))

                # -------------------------------- 推送划转失败日志 ---------------------------------------------
                # 划转失败, err_code: base-msg, err_msg: The single transfer-in amount must be no less than 0.01ETH. [划转数量：{}]
                strategy_params['information'] = '划转失败，err_code：{}，err_msg：{} [划转数量：{}{}]'.format(err_code,
                                                                                                 err_msg,
                                                                                                 _amount,
                                                                                                 currency.upper())
                strategy_params['log_type'] = 'transfer'
                self.exchanges.push_log(strategy_params, 'log')
                # -------------------------------- 推送划转失败日志 ---------------------------------------------
        except Exception as e:
            _logger.error('----- hb_transfer, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info('----- hb_transfer, 划转结果: {} -----'.format(transfer_result))
            return transfer_result

    def update_strategy_params(self, strategy_params):
        ''' 更新策略参数(现货买入数量, 系统合约持仓数量) '''
        try:
            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------
            account_id = strategy_params.get('account_id', '')
            id = strategy_params.get('id', '')

            search_dict = get_table_data(strategy_mysql_conn, const.T_FUTURES_SPOT_POSTION,
                                         ['buy_spot_amount', 'contract_positions_amount'],
                                         ['account_id', 'id'], [account_id, id])
            buy_spot_amount = search_dict.get('buy_spot_amount', 0)
            contract_positions_amount = search_dict.get('contract_positions_amount', 0)

            strategy_params['buy_spot_amount'] = float(buy_spot_amount) if buy_spot_amount else 0
            strategy_params['contract_positions_amount'] = float(
                contract_positions_amount) if contract_positions_amount else 0
            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------
        except Exception as e:
            _logger.error('----- update_strategy_params, {} -----'.format(traceback.format_exc()))
        finally:
            return strategy_params

    def buy(self, buy_dict):
        ''' 立即买入, 开仓 '''
        try:
            spot_price_precision, spot_amount_precision = '0', '0'
            futures_price_precision, futures_amount_precision = '0', '0'
            account_id = buy_dict.get('account_id', '')
            name = buy_dict.get('name', '')  # 套利组合
            buy_amount = buy_dict.get('buy_amount', 0)  # 买入金额
            buy_type = buy_dict.get('buy_type', '')  # 下单方式
            currency = buy_dict.get('currency', '')  # 币种
            exchange = buy_dict.get('exchange', '').upper()  # 交易所

            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------
            buy_dict = self.update_strategy_params(buy_dict)
            _logger.info('------ buy, 现货下单前, 取最新现货买入数量, 系统合约持仓数量, buy_dict: {} -----'.format(buy_dict))
            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------

            # ---------------------------------- 封装api配置参数 ----------------------------------
            api_config_params = self.exchanges.get_api_config_params(buy_dict)
            # ---------------------------------- 封装api配置参数 ----------------------------------

            # ---------------------------------- 查询对应币对精度 ----------------------------------
            c_buy_dict = copy.copy(buy_dict)

            # -------------- 查询现货对应币对精度
            c_buy_dict['entrust_type'] = '现货'
            spot_symbol = str(currency).lower() + 'usdt'

            if exchange == 'HB':
                self.init_hb_symbols_dict(c_buy_dict, api_config_params)

                spot_price_precision = self.symbols_dict['HB']['spot'].get(spot_symbol, {}).get('price_precision', '0')
                spot_amount_precision = self.symbols_dict['HB']['spot'].get(spot_symbol, {}).get('amount_precision',
                                                                                                 '0')
            elif exchange == 'OK':
                self.init_ok_symbols_dict(c_buy_dict, api_config_params)

                spot_price_precision = self.symbols_dict['OK']['spot'].get(spot_symbol, {}).get('price_precision', '0')
                spot_amount_precision = self.symbols_dict['OK']['spot'].get(spot_symbol, {}).get('amount_precision',
                                                                                                 '0')
            # -------------- 查询合约对应币对精度
            c_buy_dict['entrust_type'] = '合约'
            spot_symbol = str(currency).lower() + 'usd'

            if exchange == 'HB':
                self.init_hb_symbols_dict(c_buy_dict, api_config_params)

                futures_price_precision = self.symbols_dict['HB']['futures'].get(spot_symbol, {}).get('price_precision',
                                                                                                      '0')
                futures_amount_precision = self.symbols_dict['HB']['futures'].get(spot_symbol, {}).get(
                    'amount_precision', '0')
            elif exchange == 'OK':
                self.init_ok_symbols_dict(c_buy_dict, api_config_params)

                futures_price_precision = self.symbols_dict['OK']['futures'].get(spot_symbol, {}).get('price_precision',
                                                                                                      '0')
                futures_amount_precision = self.symbols_dict['OK']['futures'].get(spot_symbol, {}).get(
                    'amount_precision', '0')

            _logger.info(
                '----- buy, 查询对应的币对精度成功, params: {}, symbol_dict: {} -----'.format(buy_dict, self.symbols_dict))
            # ---------------------------------- 查询对应币对精度 ----------------------------------

            # -----------------------------***** 交易部分 ***** -----------------------------------------
            # -------------------------------- 推送开始日志 ---------------------------------------------
            buy_dict['information'] = '开始买入【{}】'.format(name)
            buy_dict['log_type'] = 'info'
            self.exchanges.push_log(buy_dict, 'log')
            # -------------------------------- 推送开始日志 ---------------------------------------------

            # -------------------------------- 现货下单 ---------------------------------------------
            order_id = ''
            trade_price = 0  # 委托价格
            trade_amount = 0  # 委托数量
            buy_dict['entrust_type'] = '现货'  # 最开始是现货下单
            symbol = str(currency).lower() + 'usdt'

            if buy_type == '最新价':
                trade_price = self.get_lasted_price(buy_dict)
                trade_amount = float(buy_amount) / float(trade_price)

            elif buy_type == '市价':
                trade_price = self.get_market_price()
                trade_amount = float(buy_amount)

            elif buy_type == '对手价':
                trade_price = self.get_market_price()
                trade_amount = float(buy_amount)

            # 封装数据
            trade_price = self.exchanges.fill_num(trade_price, spot_price_precision)
            trade_amount = self.exchanges.fill_num(trade_amount, spot_amount_precision)
            buy_dict['trade_amount'] = trade_amount  # 委托数量
            buy_dict['trade_price'] = trade_price  # 委托价格

            place_result = None
            if exchange == 'HB':
                place_result, order_id = self.hb_spot_place_order(buy_dict, api_config_params)
            elif exchange == 'OK':
                place_result, order_id = self.ok_spot_place_order(buy_dict, api_config_params)

            if not place_result:  # 下单失败, 则不继续往下操作
                return False
            # -------------------------------- 现货下单 ---------------------------------------------

            # ---------------- 循环监听现货委托成交, 全部成交 -> 下一步操作(有变化则更新持仓表和委托表) ----------------
            deal_result = None
            if exchange == 'HB':
                deal_result = self.monitor_hb_spot_order_status(buy_dict, api_config_params, str(order_id), symbol)
            elif exchange == 'OK':
                deal_result = self.monitor_ok_spot_order_status(buy_dict, api_config_params, str(order_id), symbol)

            if not deal_result:  # 未全部成交情况, 则不继续往下操作
                return False

            time.sleep(0.2)
            # ---------------- 循环监听现货委托成交, 全部成交 -> 下一步操作(有变化则更新持仓表和委托表) ----------------

            # -------------------------------------- 转账 ----------------------------------------------------
            transfer_result = None
            if exchange == 'HB':
                transfer_result = self.hb_transfer(buy_dict, api_config_params, 'pro-to-futures', buy_amount)
            elif exchange == 'OK':
                # strategy_params, api_config_params, _from, _to, _amount
                transfer_result = self.ok_transfer(buy_dict, api_config_params, '1', '3', buy_amount)

            if not transfer_result:  # 划转失败, 不继续往下操作
                return False

            time.sleep(0.2)
            # -------------------------------------- 转账 ----------------------------------------------------

            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------
            buy_dict = self.update_strategy_params(buy_dict)
            _logger.info('------ buy, 合约下单前, 取最新现货买入数量, 系统合约持仓数量, buy_dict: {} -----'.format(buy_dict))
            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------

            # -------------------------------- 合约下单 ------------------------------------------------
            order_id = ''
            trade_price = 0  # 委托价格
            trade_amount = 0  # 委托数量
            buy_dict['entrust_type'] = '合约'
            symbol = str(currency).lower() + 'usd'

            if buy_type == '最新价':
                trade_price = self.get_lasted_price(buy_dict)  # 更新
                trade_amount = self.exchanges.get_futures_entrust_amount(buy_dict, trade_price)

            elif buy_type == '市价':
                lasted_price = self.get_lasted_price(buy_dict)  # 更新
                trade_amount = self.exchanges.get_futures_entrust_amount(buy_dict, lasted_price)  # 市价用最新价算合约张数
                if exchange == 'OK':  # 如果是OK的话，市价下单需-1(买入)
                    trade_amount = float(trade_amount) - 1

                trade_price = self.get_market_price()  # 市价

            elif buy_type == '对手价':
                trade_price = self.get_lasted_price(buy_dict)  # 更新
                trade_amount = self.exchanges.get_futures_entrust_amount(buy_dict, trade_price)
                trade_price = '对手价'  # 更新

            _logger.info(
                '----- buy, 合约下单, buy_type: {}, trade_price: {}, trade_amount: {} -----'.format(buy_type,
                                                                                                trade_price,
                                                                                                trade_amount))

            # 封装数据
            trade_price = self.exchanges.fill_num(trade_price, futures_price_precision)
            trade_amount = self.exchanges.fill_num(trade_amount, futures_amount_precision)
            buy_dict['trade_amount'] = trade_amount  # 委托数量
            buy_dict['trade_price'] = trade_price  # 委托价格

            place_result = None
            if exchange == 'HB':
                place_result, order_id = self.hb_futures_place_order(buy_dict, api_config_params)
            elif exchange == 'OK':
                place_result, order_id = self.ok_futures_place_order(buy_dict, api_config_params)

            if not place_result:  # 未下单成功, 不继续操作下去
                return False
            # -------------------------------- 合约下单 ------------------------------------------------

            # -------------------- 循环监听合约委托成交, 全部成交 -> 下一步操作 -----------------------------
            if exchange == 'HB':
                deal_result = self.monitor_hb_futures_order_status(buy_dict, api_config_params, str(order_id), symbol)
            elif exchange == 'OK':
                deal_result = self.monitor_ok_futures_order_status(buy_dict, api_config_params, str(order_id), symbol)

            if not deal_result:  # 未全部成交, 不继续操作下去
                return False
            # -------------------- 循环监听合约委托成交, 全部成交 -> 下一步操作 -----------------------------

            # -------------------------------- 推送结束日志 ---------------------------------------------
            buy_dict['information'] = '【{}】买入成功'.format(name)
            buy_dict['log_type'] = 'info'
            self.exchanges.push_log(buy_dict, 'log')
            # -------------------------------- 推送结束日志 ---------------------------------------------

            # -----------------------------***** 交易部分 ***** -----------------------------------------

            # ------------------------------- 存买入成功操作日志 --------------------------------------------
            self.exchanges.futures_spot_save_operator_log(buy_dict, '成功')
            # ------------------------------- 存买入成功操作日志 --------------------------------------------
        except Exception as e:
            _logger.error('----- buy, {} -----'.format(traceback.format_exc()))

    def sell(self, sell_dict):
        ''' 立即卖出 '''
        try:
            spot_price_precision, spot_amount_precision = '0', '0'
            futures_price_precision, futures_amount_precision = '0', '0'
            sell_type = sell_dict.get('sell_type', '')  # 下单方式
            sell_amount = sell_dict.get('sell_amount', 0)  # 卖出数量
            account_id = sell_dict.get('account_id', '')
            name = sell_dict.get('name', '')  # 套利组合
            exchange = sell_dict.get('exchange', '')
            currency = sell_dict.get('currency', '')  # 币种
            sell_ratio = float(sell_dict.get('sell_ratio', 0)) / 100

            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------
            sell_dict = self.update_strategy_params(sell_dict)
            _logger.info('------ sell, 合约下单前, 取最新现货买入数量, 系统合约持仓数量, sell_dict: {} -----'.format(sell_dict))
            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------

            # ---------------------------------- 封装api配置参数 ----------------------------------
            api_config_params = self.exchanges.get_api_config_params(sell_dict)
            # ---------------------------------- 封装api配置参数 ----------------------------------

            # ---------------------------------- 查询对应币对精度 ----------------------------------
            c_sell_dict = copy.copy(sell_dict)

            # -------------- 查询现货对应币对精度
            c_sell_dict['entrust_type'] = '现货'
            spot_symbol = str(currency).lower() + 'usdt'

            if exchange == 'HB':
                self.init_hb_symbols_dict(c_sell_dict, api_config_params)

                spot_price_precision = self.symbols_dict['HB']['spot'].get(spot_symbol, {}).get('price_precision',
                                                                                                '0')
                spot_amount_precision = self.symbols_dict['HB']['spot'].get(spot_symbol, {}).get('amount_precision',
                                                                                                 '0')
            elif exchange == 'OK':
                self.init_ok_symbols_dict(c_sell_dict, api_config_params)

                spot_price_precision = self.symbols_dict['OK']['spot'].get(spot_symbol, {}).get('price_precision',
                                                                                                '0')
                spot_amount_precision = self.symbols_dict['OK']['spot'].get(spot_symbol, {}).get('amount_precision',
                                                                                                 '0')
            # -------------- 查询合约对应币对精度
            c_sell_dict['entrust_type'] = '合约'
            spot_symbol = str(currency).lower() + 'usd'

            if exchange == 'HB':
                self.init_hb_symbols_dict(c_sell_dict, api_config_params)

                futures_price_precision = self.symbols_dict['HB']['futures'].get(spot_symbol, {}).get(
                    'price_precision',
                    '0')
                futures_amount_precision = self.symbols_dict['HB']['futures'].get(spot_symbol, {}).get(
                    'amount_precision', '0')
            elif exchange == 'OK':
                self.init_ok_symbols_dict(c_sell_dict, api_config_params)

                futures_price_precision = self.symbols_dict['OK']['futures'].get(spot_symbol, {}).get(
                    'price_precision',
                    '0')
                futures_amount_precision = self.symbols_dict['OK']['futures'].get(spot_symbol, {}).get(
                    'amount_precision', '0')

            _logger.info(
                '----- sell, 查询对应的币对精度成功, params: {}, symbol_dict: {} -----'.format(sell_dict, self.symbols_dict))
            # ---------------------------------- 查询对应币对精度 ----------------------------------

            # -----------------------------***** 交易部分 ***** -----------------------------------------
            # -------------------------------- 推送开始日志 ---------------------------------------------
            sell_dict['information'] = '开始卖出【{}】'.format(name)
            sell_dict['log_type'] = 'info'
            self.exchanges.push_log(sell_dict, 'log')
            # -------------------------------- 推送开始日志 ---------------------------------------------

            # -------------------------------- 合约下单 ------------------------------------------------
            order_id = ''
            trade_price = 0  # 委托价格
            trade_amount = 0  # 委托数量
            sell_dict['entrust_type'] = '合约'
            symbol = str(currency).lower() + 'usd'

            if sell_type == '最新价':
                trade_price = self.get_lasted_price(sell_dict)  # 更新
                trade_amount = sell_amount

            elif sell_type == '市价':
                trade_amount = sell_amount
                trade_price = self.get_market_price()

            elif sell_type == '对手价':
                trade_price = '对手价'  # 更新
                trade_amount = sell_amount

            _logger.info(
                '----- sell, 合约下单, sell_type: {}, trade_price: {}, trade_amount: {} ----- -----'.format(sell_type,
                                                                                                        trade_price,
                                                                                                        trade_amount))

            # 封装数据
            trade_price = self.exchanges.fill_num(trade_price, futures_price_precision)
            trade_amount = self.exchanges.fill_num(trade_amount, futures_amount_precision)
            sell_dict['trade_amount'] = trade_amount  # 委托数量
            sell_dict['trade_price'] = trade_price  # 委托价格

            place_result = None
            if exchange == 'HB':
                place_result, order_id = self.hb_futures_place_order(sell_dict, api_config_params)
            elif exchange == 'OK':
                place_result, order_id = self.ok_futures_place_order(sell_dict, api_config_params)

            if not place_result:  # 未下单成功, 不继续操作下去
                return False
            # -------------------------------- 合约下单 ------------------------------------------------

            # --------------------------- 循环监听合约委托成交, 全部成交 -> 下一步操作 ---------------------------
            deal_result = None
            if exchange == 'HB':
                deal_result = self.monitor_hb_futures_order_status(sell_dict, api_config_params, str(order_id), symbol)
            elif exchange == 'OK':
                deal_result = self.monitor_ok_futures_order_status(sell_dict, api_config_params, str(order_id), symbol)

            if not deal_result:
                return False

            time.sleep(0.2)
            # --------------------------- 循环监听合约委托成交, 全部成交 -> 下一步操作 ---------------------------

            # -------------------------------------- 转账 ----------------------------------------------------
            transfer_result = None
            if exchange == 'HB':
                transfer_result = self.hb_transfer(sell_dict, api_config_params, 'futures-to-pro', sell_amount)
            elif exchange == 'OK':
                # strategy_params, api_config_params, _from, _to, _amount
                transfer_result = self.ok_transfer(sell_dict, api_config_params, '3', '1', sell_amount)

            if not transfer_result:  # 划转失败, 不继续往下操作
                return False

            time.sleep(0.2)
            # -------------------------------------- 转账 ----------------------------------------------------

            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------
            sell_dict = self.update_strategy_params(sell_dict)
            _logger.info('------ sell, 现货下单前, 取最新现货买入数量, 系统合约持仓数量, sell_dict: {} -----'.format(sell_dict))
            # ------------------------ 取最新现货买入数量, 系统合约持仓数量 ---------------------------

            # -------------------------------- 现货下单 ---------------------------------------------
            order_id = ''
            trade_price = 0  # 委托价格
            trade_amount = 0  # 委托数量
            sell_dict['entrust_type'] = '现货'  # 最开始是现货下单
            symbol = str(currency).lower() + 'usdt'

            # ----------- 取现货余额
            spot_balance = 0

            exchange = sell_dict.get('exchange', '')
            if exchange == 'HB':
                spot_balance = self.exchanges.get_hb_spot_balance(sell_dict)
            elif exchange == 'OK':
                spot_balance = self.exchanges.get_ok_spot_balance(sell_dict)

            if sell_type == '最新价':
                trade_price = self.get_lasted_price(sell_dict)  # 最新价
                trade_amount = float(spot_balance) * float(sell_ratio)  # 现货余额 * 卖出比例

            elif sell_type == '市价':
                trade_price = self.get_market_price()
                trade_amount = float(spot_balance) * float(sell_ratio)

            elif sell_type == '对手价':
                trade_amount = float(spot_balance) * float(sell_ratio)
                trade_price = self.get_market_price()

            # 封装数据
            trade_price = self.exchanges.fill_num(trade_price, spot_price_precision)
            trade_amount = self.exchanges.fill_num(trade_amount, spot_amount_precision)
            sell_dict['trade_amount'] = trade_amount  # 委托数量
            sell_dict['trade_price'] = trade_price  # 委托价格

            _logger.info(
                '----- sell, 现货下单, spot_balance: {}, trade_price: {}, trade_amount: {} ----- -----'.format(spot_balance,
                                                                                                           trade_price,
                                                                                                           trade_amount))

            place_result = None
            if exchange == 'HB':
                place_result, order_id = self.hb_spot_place_order(sell_dict, api_config_params)
            elif exchange == 'OK':
                place_result, order_id = self.ok_spot_place_order(sell_dict, api_config_params)

            if not place_result:  # 下单失败, 则不继续往下操作
                return False
            # -------------------------------- 现货下单 ---------------------------------------------

            # ----------------------- 循环监听现货委托成交, 全部成交 -> 下一步操作 ------------------------------
            deal_result = None
            if exchange == 'HB':
                deal_result = self.monitor_hb_spot_order_status(sell_dict, api_config_params, str(order_id), symbol)
            elif exchange == 'OK':
                deal_result = self.monitor_ok_spot_order_status(sell_dict, api_config_params, str(order_id), symbol)

            if not deal_result:  # 未全部成交情况, 则不继续往下操作
                return False
            # ----------------------- 循环监听现货委托成交, 全部成交 -> 下一步操作 ------------------------------

            # -------------------------------- 推送结束日志 ---------------------------------------------
            sell_dict['information'] = '【{}】卖出成功'.format(name)
            sell_dict['log_type'] = 'info'
            self.exchanges.push_log(sell_dict, 'log')
            # -------------------------------- 推送结束日志 ---------------------------------------------

            # -----------------------------***** 交易部分 ***** -----------------------------------------

            # ------------------------------- 存卖出成功操作日志 --------------------------------------------
            self.exchanges.futures_spot_save_operator_log(sell_dict, '成功')
            # ------------------------------- 存卖出成功操作日志 --------------------------------------------
        except Exception as e:
            _logger.error('----- sell, {} -----'.format(traceback.format_exc()))

    def __run(self, recv_message: dict):
        try:
            _logger.info('----- __run, recv_message: {} -----'.format(recv_message))
            type = recv_message.get('type')
            if type == 'buy':
                self.buy(recv_message)
            elif type == 'sell':
                self.sell(recv_message)
        except Exception as e:
            _logger.error('----- __run, {} -----'.format(traceback.format_exc()))

    def __listen_futures_spot_trade_queue(self):
        ''' 监听期现交易队列 '''
        _logger.info("----- __listen_futures_spot_trade_queue, 开始接收期现交易数据 -----")

        pubsub = master_redis_client.pubsub()
        pubsub.subscribe(const.FUTURES_SPOT_TRADE_QUEUE)
        while True:
            queue_item = pubsub.parse_response(block=True)

            if queue_item:
                if isinstance(queue_item[-1], int):
                    continue

                # 立即买入/卖出
                recv_message = json.loads(queue_item[-1])
                threading.Thread(target=self.__run, args=(recv_message,)).start()

    def start(self):
        ''' 启动 '''
        try:
            _logger.info('----- start, 启动期现交易服务 -----')
            p = Process(target=self.__listen_futures_spot_trade_queue())
            p.run()
            p.join()
        except Exception as e:
            _logger.error('----- start, {} -----'.format(traceback.format_exc()))


if __name__ == '__main__':
    obj = FuturesSpotTrade()
    params = {'type': 'sell', 'operator_id': '2c917d26757a046e01757a0750a50001', 'operator_name': 'ly',
              'operate_type': '卖出', 'operate_page': '期现持仓', 'remote_address': '113.118.75.123', 'sell_ratio': '98',
              'sell_type': '最新价', 'sell_amount': 3, 'contract_size': 5, 'name': 'OKEx-LTC当季-LTC/USDT', 'exchange': 'OK',
              'id': 29, 'account_name': 'test', 'account_id': 'test', 'spot_balance': '0.00000001',
              'future_balance': '0.02066109', 'contract_type': 'quarter', 'buy_spot_amount': 0.0,
              'contract_positions_amount': 5.0, 'information': '开始卖出【OKEx-LTC当季-LTC/USDT】', 'log_type': 'info',
              'update_time': '2021-05-12 16:48:34', 'entrust_type': '合约'}
    print(obj.get_lasted_price(params))
