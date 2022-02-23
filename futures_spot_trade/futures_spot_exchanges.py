# -*- coding: UTF-8 -*-
import copy
import re
import time
import json
import traceback

from utils.function_util import *
from utils.const import const
from strategy_order.utils.const import const as _const
from utils.config import config
from utils.log_util import logger
from utils.redis_util import master_redis_client, redis_client
from huobi.api import HuobiSpot, HuobiAccount, HuobiFutures
from okex.api_v3 import OkexRequest, OkexSpot, OkexFutures
from strategy_order.utils.process_contract_pair_name import ProcessContractType
from strategy_order.base.send_email import SendOrderDetailEmail
from strategy_order.base.util_verification import Verification

_logger = logger("futures_spot_exchanges", "futures_spot_exchanges_log", config.futures_spot__logpath)


class Exchanges:

    def __init__(self):
        self.strategy_mysql_db = config.mysql_strategy_db
        self.t_futures_spot_param = const.T_FUTURES_SPOT_PARAM  # 期现参数表
        self.t_futures_spot_postion = const.T_FUTURES_SPOT_POSTION  # 期现持仓表
        self.t_futures_spot_entrust = const.T_FUTURES_SPOT_ENTRUST  # 期现委托表
        self.t_futures_spot_deal = const.T_FUTURES_SPOT_DEAL  # 期现成交表
        self.t_futures_spot_log = const.T_FUTURES_SPOT_LOG  # 期现日志表
        self.futures_spot_log_queue = const.FUTURES_SPOT_LOG_QUEUE
        self.table_fields_dict = {}
        self.init_table_all_fileds()  # 初始化表对应的所有字段
        self.master_redis_client = master_redis_client
        self.redis_client = redis_client
        self.send_detail_email = SendOrderDetailEmail()
        self.verification = Verification()

    def init_table_all_fileds(self):
        ''' 初始化表格字段 '''
        try:
            self.table_fields_dict[self.t_futures_spot_postion] = get_all_fields(strategy_mysql_conn,
                                                                                 self.strategy_mysql_db,
                                                                                 self.t_futures_spot_postion)
            self.table_fields_dict[self.t_futures_spot_entrust] = get_all_fields(strategy_mysql_conn,
                                                                                 self.strategy_mysql_db,
                                                                                 self.t_futures_spot_entrust)
            self.table_fields_dict[self.t_futures_spot_deal] = get_all_fields(strategy_mysql_conn,
                                                                              self.strategy_mysql_db,
                                                                              self.t_futures_spot_deal)
            self.table_fields_dict[self.t_futures_spot_log] = get_all_fields(strategy_mysql_conn,
                                                                             self.strategy_mysql_db,
                                                                             self.t_futures_spot_log)
            _logger.info('----- init_table_all_fileds, 初始化表格数据完毕, {} -----'.format(self.table_fields_dict))
        except Exception as e:
            _logger.error('----- init_table_all_fileds, {} -----'.format(traceback.format_exc()))

    def get_futures_spot_log_data(self, strategy_params, _type):
        ''' 封装期现交易数据 '''
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        strategy_params['update_time'] = update_time  # 日志更新时间

        return strategy_params

    def push_log(self, strategy_params, _type=None):
        ''' 推送前端实时日志 '''
        try:
            _logger.info(
                '----- push_log, 正在推送前端日志, strategy_params: {}, _type: {}, log_type: {} -----'.format(strategy_params,
                                                                                                      _type,
                                                                                                      strategy_params.get(
                                                                                                          'log_type',
                                                                                                          '')))

            log_data = self.get_futures_spot_log_data(strategy_params, _type)  # 封装log
            self.master_redis_client.publish(self.futures_spot_log_queue,
                                             json.dumps({'type': _type, 'message': log_data}))  # 推送日志

            _logger.info('----- push_log, 推送log_data成功, {} -----'.format(log_data))

            self.save_t_futures_spot_log(log_data)  # 保存日志记录表
        except Exception as e:
            _logger.error('----- push_log, {} -----'.format(traceback.format_exc()))

    def get_lasted_futures_spot_balance(self, strategy_params):  # 针对前端调接口
        ''' 获取最近期现余额 '''
        futures_spot_balance_dict = {}
        try:
            exchange = strategy_params.get('exchange', '')
            spot_balance, future_balance = 0, 0
            if exchange.upper() == 'OK':
                future_balance = self.get_ok_futures_balance(strategy_params)

                # 现货默认查询USDT
                strategy_params['currency'] = 'USDT'
                spot_balance = self.get_ok_spot_balance(strategy_params)
            elif exchange.upper() == 'HB':
                future_balance = self.get_hb_futures_balance(strategy_params)

                # 现货默认查询USDT
                strategy_params['currency'] = 'USDT'
                spot_balance = self.get_hb_spot_balance(strategy_params)

            futures_spot_balance_dict = {'spot_balance': spot_balance, 'future_balance': future_balance}
        except Exception as e:
            _logger.error('----- get_lasted_futures_spot_balance, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info('----- get_lasted_futures_spot_balance, futures_spot_balance_dict: {} -----'.format(
                futures_spot_balance_dict))
            return futures_spot_balance_dict

    def update_t_futures_spot_postion(self, strategy_params, last_arbitrage_time=None):
        ''' 存期现持仓表 '''
        conn, cursor = None, None
        id = strategy_params.get('id', '')
        try:
            _logger.info('----- 正在存期现持仓表, strategy_params: {}, last_arbitrage_time: {} -----'.format(strategy_params,
                                                                                                     last_arbitrage_time))

            all_fields = self.table_fields_dict.get(self.t_futures_spot_postion)
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 写入时间

            connection = strategy_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            # 判断是否存在该笔数据
            search_sql = 'select id from {} where id = "{}"'.format(self.t_futures_spot_postion, id)
            cursor.execute(search_sql)
            data = cursor.fetchone()

            if not data:  # 插入数据
                insert_fields, insert_values = ['update_time'], [update_time]
                for field in strategy_params.keys():
                    if field not in insert_fields and field in all_fields:
                        value = strategy_params.get(field)

                        # 针对买入现货数量, 系统合约持仓数量
                        if field == 'buy_spot_amount':
                            if value: value = str('%.8f' % float(value))

                        if field == 'contract_positions_amount':
                            if float(value) == 0.0: value = str(int(value))

                        insert_fields.append(field)
                        insert_values.append(value)

                insert_sql = "insert into {} ({}) values {}".format(self.t_futures_spot_postion,
                                                                    ','.join(insert_fields),
                                                                    tuple(insert_values))

                _logger.info(f'----- update_t_futures_spot_postion, insert_sql: {insert_sql} -----')
                cursor.execute(insert_sql)

            else:  # 更新数据
                if not last_arbitrage_time:
                    update_fields, update_values = ['update_time'], [update_time]
                else:  # 最后一次成交会更新套利时间(结束)
                    update_fields, update_values = ['update_time', 'last_arbitrage_time'], [update_time,
                                                                                            last_arbitrage_time]

                for field in strategy_params.keys():
                    if field != 'id' and field not in update_fields and field in all_fields:
                        value = strategy_params.get(field)

                        # 针对买入现货数量, 系统合约持仓数量
                        if field == 'buy_spot_amount':
                            if value: value = str('%.8f' % float(value))

                        if field == 'contract_positions_amount':
                            if float(value) == 0.0: value = str(int(value))

                        update_fields.append(field)
                        update_values.append(value)

                update_values_str = ' , '.join(
                    list(map(lambda field, value: "%s = '%s'" % (field, value), update_fields, update_values)))
                update_sql = "update {} set {} where id = '{}'".format(self.t_futures_spot_postion,
                                                                       update_values_str,
                                                                       id)
                _logger.info(f'----- update_t_futures_spot_postion, update_sql: {update_sql} -----')

                cursor.execute(update_sql)
        except Exception as e:
            _logger.error('----- update_t_futures_spot_postion, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()
        finally:
            # 关闭连接
            conn.close()
            cursor.close()

    def update_t_futures_spot_entrust(self, strategy_params):
        ''' 更新委托表 '''
        conn, cursor, update_result = None, None, False  # 默认False
        try:
            order_id = strategy_params.get('order_id', '')
            deal_amount = strategy_params.get('deal_amount', '')

            _logger.info('----- update_t_futures_spot_entrust, strategy_params: {} ----'.format(strategy_params))

            all_fields = self.table_fields_dict.get(self.t_futures_spot_entrust)
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 写入时间

            connection = strategy_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            # 判断是否存在该笔数据
            search_sql = 'select order_id from {} where order_id = "{}"'.format(self.t_futures_spot_entrust, order_id)
            cursor.execute(search_sql)
            data = cursor.fetchone()

            if not data:  # 插入数据
                insert_fields = ['update_time', 'entrust_time']
                insert_values = [update_time, update_time]

                for field in strategy_params.keys():
                    if field not in insert_fields and field in all_fields:
                        value = strategy_params.get(field)

                        # 针对委托价格
                        if field == 'trade_price':
                            if value not in ['最新价', '市价', '对手价']:
                                if value and float(value) == 0.0: value = '市价'

                        insert_fields.append(field)
                        insert_values.append(value)

                insert_sql = "insert into {} ({}) values {}".format(self.t_futures_spot_entrust,
                                                                    ','.join(insert_fields),
                                                                    tuple(insert_values))

                _logger.info(f'----- update_t_futures_spot_entrust, insert_sql: {insert_sql} -----')
                cursor.execute(insert_sql)

            else:  # 更新
                update_fields, update_values = ['update_time', 'deal_amount'], [update_time, deal_amount]  # 更新成交数量

                for field in strategy_params.keys():
                    value = strategy_params.get(field, '')

                    if field not in update_fields and field in all_fields:
                        # 针对委托价格
                        if field == 'trade_price':
                            if value not in ['最新价', '市价', '对手价']:
                                if value and float(value) == 0.0: value = '市价'

                        update_fields.append(field)
                        update_values.append(value)

                update_values_str = ' , '.join(
                    list(map(lambda field, value: "%s = '%s'" % (field, value), update_fields, update_values)))
                update_sql = "update {} set {} where order_id = '{}'".format(self.t_futures_spot_entrust,
                                                                             update_values_str,
                                                                             order_id)

                _logger.info(f'----- update_t_futures_spot_entrust, update_sql: {update_sql} -----')
                cursor.execute(update_sql)
        except Exception as e:
            _logger.error('----- update_t_futures_spot_entrust, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()  # 提交
            update_result = True
        finally:
            # 关闭连接
            conn.close()
            cursor.close()
            return update_result

    def save_t_futures_spot_deal(self, strategy_params, order_id, deal_price, deal_amount, deal_fee, entrust_type,
                                 entrust_time,
                                 deal_time):
        ''' 存期现成交表 '''
        conn, cursor = None, None
        try:
            _logger.info(
                '----- save_t_futures_spot_deal, 正在存期现成交表, order_id: {}, strategy_params: {} -----'.format(order_id,
                                                                                                           strategy_params))

            connection = strategy_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            search_sql = "select order_id from {} where order_id = '{}'".format(self.t_futures_spot_deal,
                                                                                order_id)
            cursor.execute(search_sql)
            datas = cursor.fetchall()

            if datas:  # 如果数据存在
                _logger.info(f'----- save_t_futures_spot_deal, 该数据存在, {search_sql}, 请检查... -----')
            else:
                update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 写入时间
                all_fields = self.table_fields_dict.get(self.t_futures_spot_deal)

                insert_fields = ['order_id', 'update_time', 'deal_amount', 'deal_price', 'entrust_type', 'deal_fee',
                                 'entrust_time', 'deal_time']
                insert_values = [order_id, update_time, deal_amount, deal_price, entrust_type, deal_fee, entrust_time,
                                 deal_time]

                for field in strategy_params.keys():
                    value = strategy_params.get(field, '')

                    if field not in insert_fields and field in all_fields:
                        # 针对委托价格
                        if field == 'trade_price':
                            if value not in ['最新价', '市价', '对手价']:
                                if value and float(value) == 0.0: value = '市价'

                        insert_fields.append(field)
                        insert_values.append(value)

                insert_sql = "insert into {} ({}) values {}".format(self.t_futures_spot_deal,
                                                                    ','.join(insert_fields),
                                                                    tuple(insert_values))
                cursor.execute(insert_sql)

                # 删除委托表原数据
                delete_sql = 'delete from {} where order_id = "{}"'.format(self.t_futures_spot_entrust, order_id)
                cursor.execute(delete_sql)
        except Exception as e:
            _logger.error('----- save_t_futures_spot_deal, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()
        finally:
            # 关闭连接
            conn.close()
            cursor.close()

    def save_t_futures_spot_log(self, strategy_params):
        ''' 存期现日志表 '''
        conn, cursor = None, None
        try:
            _logger.info('----- save_t_futures_spot_log, 正在保存期现日志表, strategy_params: {} -----'.format(strategy_params))

            all_fields = self.table_fields_dict.get(self.t_futures_spot_log)
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 写入时间

            connection = strategy_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            insert_fields, insert_values = ['update_time'], [update_time]
            for field in strategy_params.keys():
                value = strategy_params.get(field)

                if field not in insert_fields and field in all_fields:
                    insert_fields.append(field)
                    insert_values.append(value)

            insert_sql = "insert into {} ({}) values {}".format(self.t_futures_spot_log,
                                                                ','.join(insert_fields),
                                                                tuple(insert_values))
            _logger.info('---- save_t_futures_spot_log, insert_sql: {} -----'.format(insert_sql))

            cursor.execute(insert_sql)
        except Exception as e:
            _logger.error('----- update_t_futures_spot_log, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()  # 提交
        finally:
            cursor.close()
            conn.close()

    def get_lasted_contract_positions_amount(self, strategy_params):
        ''' redis取最新的合约持仓数量 '''
        lasted_amount = 0
        try:
            account_name = strategy_params.get('account_name', '')
            search_result_dict = get_table_data(strategy_mysql_conn, 'account_manager',
                                                ['account_name', 'delivery_id', 'account_id'],
                                                ['account_name'], [account_name])

            if search_result_dict:
                user_id = delivery_id = search_result_dict.get('delivery_id', '')
                user_id, currency, deposit_type = user_id, strategy_params.get(
                    'currency'), strategy_params.get('deposit_type')
                key = currency if deposit_type.upper().strip() != 'USDT' else '{}-{}'.format(currency, deposit_type)
                value = self.redis_client.hget('assets_{}'.format(user_id), key)

                if value:
                    value = json.loads(value)
                    lasted_amount = value.get('total', 0)
        except Exception as e:
            _logger.error('----- get_contract_positions_amount, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info('----- get_contract_positions_amount, strategy_params: {}, lasted_amount: {} -----'.format(
                strategy_params,
                lasted_amount))
            return lasted_amount

    def get_api_config_params(self, strategy_params):
        ''' 获取api配置参数 '''
        api_dict = {}
        try:
            account_id = strategy_params.get('account_id', '')
            exchange = strategy_params.get('exchange', '')
            api_config_params_data = self.get_account_manager_data(account_id)
            access_key, secret_key, pass_phrase = api_config_params_data[0], api_config_params_data[1], \
                                                  api_config_params_data[2]

            if exchange.upper() == 'OK':
                api_dict = {'access_key': access_key, 'secret_key': secret_key, 'pass_phrase': pass_phrase}
            elif exchange.upper() == 'HB':
                api_dict = {'access_key': access_key, 'secret_key': secret_key, 'futures_url': 'https://api.hbdm.com',
                            'spot_url': 'https://api.huobi.pro'}
        except Exception as e:
            _logger.error('----- get_api_config_params, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info('----- get_api_config_params, strategy_params: {}, api_dict: {} -----'.format(strategy_params,
                                                                                                       api_dict))

            return api_dict

    def get_account_manager_data(self, account_id: str):
        ''' 查询账号管理表数据 '''
        api_config_params_data, conn, cursor = {}, None, None
        try:
            _logger.info('----- get_account_manager_data, 正在查询account_manage, account_id: {} -----'.format(account_id))

            connection = strategy_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            search_fields = ['access_key', 'secret_key', 'pass_phrase']
            search_sql = "select {} from {} where account_id = '{}';".format(', '.join(search_fields),
                                                                             _const.ACCOUNT_MANAGER, account_id)
            cursor.execute(search_sql)
            api_config_params_data = cursor.fetchone()
        except Exception as e:
            _logger.error('----- get_account_manager_data, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()  # 提交
        finally:
            # 关闭连接
            conn.close()
            cursor.close()
            _logger.info(
                '----- get_account_manager_data, api_config_params_data: {} -----'.format(api_config_params_data))
            return api_config_params_data

    def get_ok_spot_balance(self, strategy_params):
        ''' 获取ok账户现货余额 '''
        balance = 0
        try:
            currency = strategy_params.get('currency', '')

            # 获取api配置参数
            api_dict = self.get_api_config_params(strategy_params)
            access_key = api_dict.get('access_key', '')
            secret_key = api_dict.get('secret_key', '')
            pass_phrase = api_dict.get('pass_phrase', '')

            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_spot = OkexSpot(req)

            api_result = okex_spot.get_wallet_currency(currency)

            if api_result:
                balance = api_result.get('balance', 0)
        except Exception as e:
            _logger.error('----- get_ok_spot_balance, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info(
                '----- get_ok_spot_balance, strategy_params: {}, balance: {} -----'.format(strategy_params, balance))
            return balance

    def get_hb_spot_balance(self, strategy_params):
        ''' 获取hb账户现货余额 '''
        balance = 0
        try:
            # 获取api配置参数
            api_dict = self.get_api_config_params(strategy_params)
            URL = api_dict.get('spot_url', '')  # 现货URL
            ACCESS_KEY = api_dict.get('access_key', '')
            SECRET_KEY = api_dict.get('secret_key', '')

            huobi_account = HuobiAccount(URL, ACCESS_KEY, SECRET_KEY)
            api_result = huobi_account.get_accounts()
            _logger.info('----- get_hb_spot_balance, 查账户id, api_result: {} -----'.format(api_result))

            account_datas = api_result.get('data', [])
            if account_datas:
                account_id = ''

                for account_data in account_datas:
                    account_id = account_data.get('id', '')
                    account_type = account_data.get('type', '')

                    if account_type.lower() == 'spot':
                        _logger.info(
                            '----- get_hb_spot_balance, 查询account_id成功, account_id: {} -----'.format(account_id))
                        break

                if account_id:
                    # account_id查询账户余额(例 16629600)
                    huobi_spot = HuobiSpot(URL, ACCESS_KEY, SECRET_KEY)

                    api_result = huobi_spot.get_account_balance(account_id)

                    _logger.info('----- get_hb_spot_balance, 查账户余额, api_result: {} -----'.format(api_result))

                    all_datas = api_result.get('data', {}).get('list', [])
                    if all_datas:
                        currency = strategy_params.get('currency', '').lower()
                        for data in all_datas:
                            cur_currency = data.get('currency', '')
                            cur_type = data.get('type', '')

                            if cur_currency.lower() == currency.lower() and cur_type.lower() == 'trade':
                                balance = data.get('balance', 0)
                                break
        except Exception as e:
            _logger.error('----- get_hb_spot_balance, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info(
                '----- get_hb_spot_balance, strategy_params: {}, balance: {} ----'.format(strategy_params, balance))
            return balance

    def get_ok_futures_balance(self, strategy_params):
        ''' 获取ok合约账户余额 '''
        balance = 0
        try:
            currency = strategy_params.get('currency', '')
            underlying = '{}-{}'.format(currency.lower(), 'usd')

            # 获取api配置参数
            api_dict = self.get_api_config_params(strategy_params)
            access_key = api_dict.get('access_key', '')
            secret_key = api_dict.get('secret_key', '')
            pass_phrase = api_dict.get('pass_phrase', '')

            _logger.info(
                '----- get_ok_futures_balance, api请求参数, api_key: {}, api_seceret_key: {}, passphrase: {} -----'.format(
                    access_key, secret_key,
                    pass_phrase))
            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_futures = OkexFutures(req)

            api_result = okex_futures.get_accounts_info(underlying=underlying)
            _logger.info('----- get_ok_futures_balance, api返回结果, api_result: {} -----'.format(api_result))
            if api_result:
                balance = api_result.get('can_withdraw', 0)
        except Exception as e:
            _logger.error('----- get_ok_futures_balance, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info(
                '----- get_ok_futures_balance, strategy_params: {}, balance: {} -----'.format(strategy_params, balance))
            return balance

    def get_hb_futures_balance(self, strategy_params):
        ''' 获取hb合约账户余额 '''
        balance = 0
        try:
            currency = strategy_params.get('currency', '')

            # 获取api配置参数
            api_dict = self.get_api_config_params(strategy_params)
            URL = api_dict.get('futures_url', '')  # 现货URL
            ACCESS_KEY = api_dict.get('access_key', '')
            SECRET_KEY = api_dict.get('secret_key', '')

            _logger.info(
                '----- get_hb_futures_balance, api请求参数, ACCESS_KEY: {}, SECRET_KEY: {}, symbol: {} -----'.format(
                    ACCESS_KEY, SECRET_KEY, currency))
            huobi_futures = HuobiFutures(URL, ACCESS_KEY, SECRET_KEY)
            api_result = huobi_futures.get_contract_account_info(symbol=currency)
            _logger.info('----- get_hb_futures_balance, api返回结果, api_result: {} -----'.format(api_result))

            status = api_result.get('status', '')
            account_datas = api_result.get('data', [])
            if status.lower() == 'ok' and account_datas:
                data = account_datas[0]
                balance = data.get('withdraw_available', 0)  # 可划转数量
        except Exception as e:
            _logger.error('----- get_hb_futures_balance, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info(
                '----- get_hb_futures_balance, strategy_params: {}, balance: {} ----'.format(strategy_params, balance))
            return balance

    def get_instrument_id(self, strategy_params: dict):
        """
        获取instrument_id
        :param strategy_params: 策略参数
        :return:
        """
        exchange = str(strategy_params.get('exchange', ''))
        currency = strategy_params.get('currency', '')
        # deposit_type = strategy_params.get('deposit_type', '')
        deposit_type = 'USD'  # 期货默认USD
        pair_id = self.get_pair_id(strategy_params)
        contract_time = self.get_contract_date(pair_id)
        if exchange.upper() == 'HB':
            return currency

        if exchange.upper() == 'OK':
            return '{}-{}-{}'.format(currency, deposit_type, contract_time)

    def get_pair_id(self, strategy_params):
        ''' 查询pair_id '''
        conn, cursor, strategy_key, pair_id, v_sql = None, None, '', '', ''
        try:
            _logger.info('----- get_pair_id, strategy_params: {} ------', strategy_params)

            contract_time = strategy_params.get('contract_type', '')
            exchange = strategy_params.get('exchange', '')
            currency = strategy_params.get('currency', '')
            entrust_type = strategy_params.get('entrust_type', '')
            exchange_id = ''

            if entrust_type == '现货':
                deposit_type = 'USDT'  # 现货
                goods_type = 1
                currency = currency.lower()

                if exchange.upper() == 'HB':  # hb
                    strategy_key = '{}{}'.format(currency, deposit_type)
                    exchange_id = 1001
                elif exchange.upper() == 'OK':  # ok
                    strategy_key = '{}-{}'.format(currency, deposit_type)
                    exchange_id = 1002
            else:
                deposit_type = 'USD'  # 期货
                goods_type = 2

                if exchange.upper() == 'HB':  # hb
                    strategy_key = '{}{}'.format(currency, contract_time)
                    exchange_id = 1001
                elif exchange.upper() == 'OK':  # ok
                    strategy_key = '{}-{}-{}'.format(currency, deposit_type, contract_time)
                    exchange_id = 1002

            connection = trade_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            # 根据contract_time反推出时间，根据时间在variety_info表中查找出pair_id
            try:  # 期货
                search_fields = ['id', 'instrument_id']
                v_sql = 'select {} from variety_info where goods_type={} and exchange_id = "{}" and instrument_id like "{}%"'.format(
                    ','.join(search_fields), goods_type, exchange_id, currency)
                _logger.info('----- get_pair_id, 查询v_sql, v_sql: {} -----'.format(v_sql))
                cursor.execute(v_sql)
                result = cursor.fetchall()

                for item in result:
                    instrument_id = item[1]

                    if entrust_type != '现货':
                        contract_date = re.findall('\d{6}', instrument_id)
                        if len(contract_date):
                            contract_type = ProcessContractType().get_contract_name({'pair': contract_date[0]})
                            if not contract_type:
                                continue

                            trans_instrument = re.sub(contract_date[0], _const.contract_name_dict[contract_type],
                                                      instrument_id)

                            if strategy_key.upper() == trans_instrument.upper():
                                pair_id = item[0]
                    else:  # 现货
                        if strategy_key.upper() == instrument_id.upper():
                            pair_id = item[0]
            except Exception as e:
                _logger.error('----- get_pair_id, -----'.format(traceback.format_exc()))
        except Exception as e:
            _logger.error('----- get_pair_id -----'.format(traceback.format_exc()))
        finally:
            _logger.info('----- get_pair_id, pair_id: {} -----'.format(pair_id))
            return pair_id

    def get_contract_date(self, pair_id):
        ''' 获取合约日期 '''
        conn, cursor, contract_date_result = None, None, ''
        try:
            connection = trade_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            sql = 'select instrument_id from variety_info where id=%s' % pair_id
            _logger.info('----- get_contract_date, sql: {} -----'.format(sql))
            cursor.execute(sql)
        except Exception as e:
            _logger.error('----- get_contract_date, {} -----'.format(traceback.format_exc()))
        else:
            result = cursor.fetchall()
            if len(result):
                instrument_id = result[0][0]
                if re.search('\d', instrument_id):
                    contract_date = re.findall('\d{6}', instrument_id)
                    contract_date_result = contract_date[0]
        finally:
            cursor.close()
            conn.close()

            _logger.info(
                '----- get_contract_date, pair_id: {}, contract_date: {} -----'.format(pair_id, contract_date_result))
            return contract_date_result

    def get_user_id(self, account_id):
        search_dict = get_table_data(strategy_mysql_conn, 'account_manager', ['delivery_id', 'stock_id'],
                                     ['account_id'], [account_id])
        delivery_id = search_dict.get('delivery_id')
        stock_id = search_dict.get('stock_id')

        _logger.info('----- get_user_id, delivery_id: {} stock_id: {} -----'.format(delivery_id, stock_id))
        return delivery_id, stock_id

    def get_assets(self, strategy_params: dict):
        ''' 获取资产 '''
        values = dict()
        try:
            deposit_type = 'USD'
            account_id = strategy_params.get('account_id', '')
            currency = strategy_params.get('currency', '')
            delivery_id, stock_id = self.get_user_id(account_id)  # delivery_id 期货id(合约id) stock_id 现货id

            key = currency if deposit_type.upper().strip() != 'USDT' else '{}-{}'.format(currency, deposit_type)

            values = self.redis_client.hget('assets_{}'.format(delivery_id), key)
            if values:
                values = json.loads(values)
        except Exception as e:
            _logger.error('----- get_assets, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info('----- get_assets, values: {} -----'.format(values))
            return values if values else {}

    def get_face_value(self, strategy_params):
        """
        根据参数查找contract_unit(只有合约才需要查找面值, good_type=2)
        """
        # 使用交易的库
        _conns, v_sql = trade_mysql_conn(), ''
        strategy_key, contract_unit = '', ''
        trade_conn, trade_cursor = _conns['conn'], _conns['cursor']
        try:
            _logger.info('----- get_face_value, strategy_params: {} -----'.format(strategy_params))
            exchange = strategy_params.get('exchange', '')
            exchange_id = 0

            if exchange.upper() == 'HB':
                exchange_id = 1001
            elif exchange.upper() == 'OK':
                exchange_id = 1002

            currency = strategy_params.get('currency', '')
            deposit_type = 'USD'  # TODO 期现策略只有合约才需要查面值, 保证金类型默认是USD
            contract_time = strategy_params.get('contract_type', '')

            if str(exchange_id) == '1001':  # hb
                strategy_key = '{}{}'.format(currency, contract_time)
            elif str(exchange_id) == '1002':
                strategy_key = '{}-{}-{}'.format(currency, deposit_type, contract_time)

            # 根据contract_time反推出时间，根据时间在variety_info表中查找出pair_id
            search_fields = ['instrument_id', 'contract_unit']
            v_sql = 'select {} from variety_info where goods_type=2 and exchange_id = "{}" and instrument_id like "{}%"'.format(
                ','.join(search_fields), exchange_id, currency)

            _logger.info('----- get_face_value, v_sql: {} -----'.format(v_sql))

            trade_cursor.execute(v_sql)
            result = trade_cursor.fetchall()
            for item in result:
                instrument_id = item[0]
                contract_date = re.findall('\d{6}', instrument_id)

                if len(contract_date):
                    contract_type = ProcessContractType().get_contract_name({'pair': contract_date[0]})
                    if not contract_type:
                        continue
                    trans_instrument = re.sub(contract_date[0], _const.contract_name_dict[contract_type],
                                              instrument_id)
                    if strategy_key.lower() == trans_instrument.lower():
                        contract_unit = item[1]
        except Exception as e:
            _logger.error('----- get_face_value, {} -----'.format(traceback.format_exc()))
        finally:
            trade_cursor.close()
            trade_conn.close()

            _logger.info('----- get_face_value, contract_unit: {} -----'.format(contract_unit))
            return contract_unit

    def get_futures_entrust_amount(self, strategy_params, trade_price):
        ''' 可开张数=可用余额*合约价格*杠杆倍数/面值，取整 '''
        num = 0
        try:
            _logger.info('----- get_futures_entrust_amount, 计算可开张数, strategy_params: {}, trade_price: {} ----'.format(
                strategy_params, trade_price))

            face_value = self.get_face_value(strategy_params)

            # ----------------------------------- 调接口查询余额 ----------------------------------
            available = 0
            exchange = strategy_params.get('exchange', '')
            entrust_type = strategy_params.get('entrust_type', '')
            if exchange.upper() == 'OK':
                if entrust_type == '现货':
                    available = self.get_ok_spot_balance(strategy_params)
                elif entrust_type == '合约':
                    available = self.get_ok_futures_balance(strategy_params)
            elif exchange.upper() == 'HB':
                if entrust_type == '现货':
                    available = self.get_hb_spot_balance(strategy_params)
                elif entrust_type == '合约':
                    available = self.get_hb_futures_balance(strategy_params)

            # ----------------------------------- 调接口查询余额 ----------------------------------

            num = int((float(available) * float(trade_price) * 1) / float(face_value))

            _logger.info(
                '----- get_futures_entrust_amount, face_value: {}, available: {} -----'.format(face_value, available))
        except Exception as e:
            _logger.error('----- get_futures_entrust_amount, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info('----- get_futures_entrust_amount, 可开张数为-num--{} -----'.format(num))
            return num

    def handler_email_content(self, email_detail):
        ''' 处理邮件内容格式 '''
        email_html = ''
        try:
            email_html = '<table width="600" border="1">'
            email_content_html = ''

            email_detail_list = email_detail.split('\r\n')
            for email_detail_str in email_detail_list:
                email_field = email_detail_str.split('：')[0]
                email_content = email_detail_str.split('：')[1]

                email_content_html = email_content_html + '\n''<tr><td width= "50%" style="font-size: 16px;">{}</td><td width= "50%" style="font-size: 16px;">{}</td></tr>'.format(
                    email_field,
                    email_content)
            email_html = email_html + email_content_html + '\n' + '</table>'
        except Exception as e:
            _logger.error('----- handler_email_content, {} -----'.format(traceback.format_exc()))
            print('----- handler_email_content, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info(
                '----- handler_email_content, 处理邮件格式成功: email_detail: {}, email_html: {} ----'.format(email_detail,
                                                                                                      email_html))
            return email_html

    @staticmethod
    def timestamp_to_date(timestamp):
        ''' 时间戳转日期 '''
        try:
            datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))
            return datetime
        except Exception as e:
            _logger.error(
                '----- 时间戳: {}, --timestamp_to_date, {} -----'.format(timestamp, traceback.format_exc()))

    def send_grid_trade_email(self, email_content='', email_list=None, title=''):
        ''' 交易成交, 发送邮件 '''
        try:
            self.send_detail_email.send_message(email_content, email_list, title)
            _logger.info(
                '----- send_grid_trade_email, 发送期现交易成功email成功, email_content：{}, email_list: {}, title: {} ----'.format(
                    email_content,
                    email_list,
                    title))
        except Exception as e:
            _logger.error('----- send_grid_trade_email, {} -----'.format(traceback.format_exc()))

    def fill_num(self, num, count):
        try:
            _logger.info('----- fill_num, num: {}, count: {} -----'.format(num, count))

            if num in ['对手价', '市价', '最新价']:
                _logger.info('---- fill_num, 不处理, num: {} -----'.format(num))
                return num

            if re.search('e', str(num)) or re.search('E', str(num)):
                num = '%.8f' % float(num)  # 避免科学计数法, float转的时候四舍五入, 按最大价格精度8位来取
                _logger.info('----- fill_num, 针对科学计数法, 保留最大价格精度8位, num: {} -----'.format(num))

            num, count = str(num), int(count)
            num_list = num.split('.')
            if len(num_list) == 2:  # 期货张数无需转格式
                pre_num = num.split('.')[0]
                after_num = num.split('.')[1]

                if len(after_num) < count:
                    after_num = after_num + '0' * (count - len(after_num))

                if count != 0:
                    new_num = pre_num + '.' + after_num[: count]
                else:
                    new_num = pre_num

            else:
                new_num = num

            _logger.info('----- fill_num, 转换结果: {} -----'.format(new_num))
            return new_num
        except Exception as e:
            _logger.error('----- fill_num, {} -----'.format(traceback.format_exc()))

    def get_strategy_param(self):
        ''' 获取间隔时间 '''
        strategy_param = {}
        try:
            strategy_param = self.master_redis_client.get(const.STRATEGY_PARAM)
            if strategy_param:
                strategy_param = json.loads(strategy_param)
        except Exception as e:
            _logger.error('----- get_strategy_param, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info(f'----- get_strategy_param: {strategy_param} -----')
            return strategy_param if strategy_param else {}

    def delete_entrust_data(self, order_id):
        conn, cursor, delete_result = None, None, False
        try:
            connection = strategy_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            search_sql = "select order_id from {} where order_id = '{}'".format(self.t_futures_spot_entrust,
                                                                                order_id)
            cursor.execute(search_sql)
            datas = cursor.fetchall()

            if not datas:  # 如果数据不存在
                _logger.info(f'----- delete_entrust_data, 该数据不存在, {search_sql}, 请检查... -----')
            else:
                # 删除委托表原数据
                delete_sql = 'delete from {} where order_id = "{}"'.format(self.t_futures_spot_entrust, order_id)
                cursor.execute(delete_sql)
        except Exception as e:
            _logger.error('----- delete_entrust_data, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()
            delete_result = True
        finally:
            _logger.info('----- delete_entrust_data, order_id: {} -----'.format(order_id))
            # 关闭连接
            conn.close()
            cursor.close()

            return delete_result

    def get_futures_spot_id_name(self, exchange, currency, contract_type):
        ''' 获取id '''
        search_dict = get_table_data(strategy_mysql_conn, self.t_futures_spot_param, ['id', 'name'],
                                     ['exchange', 'currency', 'contract_type'], [exchange, currency, contract_type])
        _logger.info(
            '----- get_futures_spot_id_name, exchange: {}, currency: {}, contract_type: {}, search_dict: {} -----'.format(
                exchange, currency, contract_type, search_dict))

        futures_spot_id = search_dict.get('id', '')
        futures_spot_name = search_dict.get('name', '')

        if not futures_spot_id and not futures_spot_name:
            # 取持仓表匹配, 看是否存在
            search_dict = get_table_data(strategy_mysql_conn, self.t_futures_spot_postion, ['id', 'name'],
                                         ['exchange', 'currency', 'contract_type'], [exchange, currency, contract_type])
            futures_spot_id = search_dict.get('id', '')
            futures_spot_name = search_dict.get('name', '')

        _logger.info(
            '----- get_futures_spot_id_name, futures_spot_id: {}, futures_spot_name: {} -----'.format(futures_spot_id,
                                                                                                      futures_spot_name))
        return futures_spot_id, futures_spot_name

    def update_ok_futures_position(self, account_params):
        ''' 更新ok用户持仓数据(其他平台的持仓数据) '''
        try:
            _logger.info(
                '----- update_ok_futures_position, account_params: {} -----'.format(account_params))
            # 获取api配置参数
            api_dict = self.get_api_config_params(account_params)
            access_key = api_dict.get('access_key', '')
            secret_key = api_dict.get('secret_key', '')
            pass_phrase = api_dict.get('pass_phrase', '')

            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_futures = OkexFutures(req)

            api_result = okex_futures.get_futures_position()

            # 取全仓数据
            crossed_api_result = api_result
            _logger.info('----- update_ok_futures_position, api返回结果, crossed_api_result: {}, type: {} -----'.format(
                crossed_api_result, type(api_result)))

            result = crossed_api_result.get('result', '')
            if result == True and crossed_api_result:
                holding = crossed_api_result.get('holding', [])
                holding_datas = holding[0]

                for holding_data in holding_datas:
                    short_qty = holding_data.get('short_qty', 0)
                    if short_qty and float(short_qty) > 0:
                        instrument_id = holding_data.get('instrument_id', '')  # BTC-USD-191018
                        currency = str(instrument_id.split('-')[0]).upper()
                        deposit_type = str(instrument_id.split('-')[1]).upper()
                        if deposit_type != 'USD':
                            continue  # 合约默认都是USD

                        contract_time = instrument_id.split('-')[2]
                        contract_type_name = ProcessContractType().get_contract_name({'pair': contract_time})
                        contract_type_dict = {'当季': 'quarter', '次季': 'bi_quarter', '当周': 'this_week', '次周': 'next_week'}
                        contract_type = contract_type_dict.get(contract_type_name)
                        id, name = self.get_futures_spot_id_name('OK', currency, contract_type)

                        if not id:
                            id = self.generate_futures_spot_id()  # 如果在参数表没有找到的话, 则生成一个新的持仓数据(id = id + 1)
                            name = '{}-{}-{}'.format('OKEx', str(currency).upper() + contract_type_name,
                                                     str(currency).upper() + '/' + 'USDT')

                        account_name = account_params.get('account_name', '')
                        account_id = account_params.get('account_id', '')

                        strategy_params = {
                            'id': id,
                            'name': name,
                            'exchange': 'OK',
                            'contract_type': contract_type,
                            'account_name': account_name,
                            'account_id': account_id,
                            'currency': currency
                        }

                        self.update_t_futures_spot_postion(strategy_params)  # 存持仓表
                        _logger.info('----- update_ok_futures_position, 存持仓表成功, strategy_params: {} -----'.format(
                            strategy_params))
        except Exception as e:
            _logger.error('----- update_ok_futures_position, {} -----'.format(traceback.format_exc()))

    def update_hb_futures_position(self, account_params):
        ''' 更新hb用户持仓数据(其他平台的持仓数据) '''
        try:
            _logger.info(
                '----- update_hb_futures_position, account_params: {} -----'.format(account_params))
            # 获取api配置参数
            api_dict = self.get_api_config_params(account_params)
            URL = api_dict.get('futures_url', '')  # 期货URL
            ACCESS_KEY = api_dict.get('access_key', '')
            SECRET_KEY = api_dict.get('secret_key', '')

            _logger.info(
                '----- update_hb_futures_position, api请求参数, ACCESS_KEY: {}, SECRET_KEY: {} -----'.format(
                    ACCESS_KEY, SECRET_KEY))
            huobi_futures = HuobiFutures(URL, ACCESS_KEY, SECRET_KEY)
            api_result = huobi_futures.get_contract_position_info()
            _logger.info('----- update_hb_futures_position, api返回结果, api_result: {} -----'.format(api_result))

            status = api_result.get('status', '')
            position_datas = api_result.get('data', [])
            if status.lower() == 'ok' and position_datas:
                for position_data in position_datas:
                    direction = position_data.get('direction', '')
                    if direction.lower() != 'sell':
                        continue  # 只取sell, 空头持仓

                    contract_type = position_data.get('contract_type', '')
                    contract_type_dict = {'quarter': '当季', 'bi_quarter': '次季', 'this_week': '当周', 'next_week': '次周'}
                    volume = position_data.get('volume', 0)
                    currency = position_data.get('symbol').upper()

                    if volume and float(volume) > 0:
                        account_name = account_params.get('account_name', '')
                        account_id = account_params.get('account_id', '')

                        # 针对contract_type
                        if contract_type.strip() == 'next_quarter':
                            contract_type = 'bi_quarter'

                        id, name = self.get_futures_spot_id_name('HB', currency, contract_type)

                        if not id:
                            contract_type_name = contract_type_dict.get(contract_type)
                            id = self.generate_futures_spot_id()  # 如果在参数表没有找到的话, 则生成一个新的持仓数据(id = id + 1)
                            name = '{}-{}-{}'.format('火币网', str(currency).upper() + contract_type_name,
                                                     str(currency).upper() + '/' + 'USDT')

                        strategy_params = {
                            'id': id,
                            'name': name,
                            'exchange': 'HB',
                            'contract_type': contract_type,
                            'account_name': account_name,
                            'account_id': account_id,
                            'currency': currency
                        }

                        self.update_t_futures_spot_postion(strategy_params)  # 存持仓表
                        _logger.info('----- update_hb_futures_position, 存持仓表成功, strategy_params: {} -----'.format(
                            strategy_params))
        except Exception as e:
            _logger.error('----- update_hb_futures_position, {} -----'.format(traceback.format_exc()))

    def delete_position_data(self):
        ''' 清除系统合约持仓数量0的数据 '''
        conn, cursor = None, None
        try:
            connection = strategy_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            delete_sql = 'delete from {} where contract_positions_amount = "0" or contract_positions_amount is NULL'.format(
                self.t_futures_spot_postion)
            _logger.info('----- delete_position_data, delete_sql: {} -----'.format(delete_sql))
            cursor.execute(delete_sql)

        except Exception as e:
            _logger.error('----- delete_position_data, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()
        finally:
            # 关闭连接
            conn.close()
            cursor.close()

    def generate_futures_spot_id(self):
        ''' 生成id '''
        new_futures_spot_id = None
        try:
            all_ids_list = []
            params_ids_list = get_all_data(strategy_mysql_conn, self.t_futures_spot_param, ['id'])
            position_ids_list = get_all_data(strategy_mysql_conn, self.t_futures_spot_postion, ['id'])

            all_ids_list.extend([item.get('id', '') for item in params_ids_list])
            all_ids_list.extend([item.get('id', '') for item in position_ids_list])

            if all_ids_list:
                _logger.info(
                    '----- generate_futures_spot_id, all_ids_list: {} -----'.format(all_ids_list))
                new_futures_spot_id = max(all_ids_list) + 1

        except Exception as e:
            _logger.error('----- generate_futures_spot_id, {} -----'.format(traceback.format_exc()))
        finally:
            # 关闭连接
            _logger.info('----- generate_futures_spot_id, new_futures_spot_id: {} -----'.format(new_futures_spot_id))
            return new_futures_spot_id

    def ok_spot_cancel_order(self, strategy_params):
        ''' ok现货撤单 '''
        cancel_result, error_code = '', ''
        try:
            order_id = strategy_params.get('order_id', '')
            currency = strategy_params.get('currency', '')
            _instrument_id = '{}-{}'.format(currency.lower(), 'usdt')

            _logger.info(
                '----- ok_spot_cancel_order, order_id: {}, strategy_params: {} -----'.format(order_id, strategy_params))
            # 获取api配置参数
            api_dict = self.get_api_config_params(strategy_params)
            access_key = api_dict.get('access_key', '')
            secret_key = api_dict.get('secret_key', '')
            pass_phrase = api_dict.get('pass_phrase', '')

            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_spot = OkexSpot(req)

            _logger.info(
                '----- ok_spot_cancel_order, api请求参数, api_key: {}, api_seceret_key: {}, passphrase: {}, instrument_id: {}, order_id: {} -----'.format(
                    access_key, secret_key, pass_phrase, _instrument_id, order_id))
            api_result = okex_spot.cancel_order(instrument_id=_instrument_id, order_id=order_id)  # btc-usdt
            _logger.info('----- ok_spot_cancel_order, api_result: {} -----'.format(api_result))

            error_message = api_result.get('error_message', '')
            error_code = api_result.get('error_code', '')
            cancel_result = api_result.get('result', '')

            trade_type = strategy_params.get('trade_type', '')
            entrust_price = strategy_params.get('entrust_price', '0')
            entrust_amount = strategy_params.get('entrust_amount', '0')
            part_deal_amount = strategy_params.get('part_deal_amount', '')
            if part_deal_amount:
                entrust_amount = float(entrust_amount) - float(part_deal_amount)

            if cancel_result == True and str(error_code) == '0':  # TODO 撤单后不推撤单日志, 监听到撤单推
                # # ------------------------- 推送撤单成功日志 -------------------------------------
                # information = '{}，委托价格：{}，委托数量：{}，订单编号：{}'.format(trade_type, entrust_price, entrust_amount, order_id)
                # strategy_params['information'] = information
                # strategy_params['log_type'] = 'cancel'
                # self.push_log(strategy_params, 'log')
                # # ------------------------- 推送撤单成功日志 -------------------------------------

                self.delete_entrust_data(order_id)  # 撤单成功, 删委托
            else:
                # ------------------------- 推送撤单失败日志 -------------------------------------
                if not error_message: error_message = error_code
                information = '{}撤单失败，委托价格：{}，委托数量：{}，订单编号：{} [{}]'.format(trade_type, entrust_price, entrust_amount,
                                                                           order_id, error_message)
                strategy_params['information'] = information  # info
                strategy_params['log_type'] = 'info'
                self.push_log(strategy_params, 'log')
                # ------------------------- 推送撤单失败日志 -------------------------------------
        except Exception as e:
            _logger.error('----- ok_spot_cancel_order, {} -----'.format(traceback.format_exc()))
        finally:
            return cancel_result, error_code

    def hb_spot_cancel_order(self, strategy_params):
        ''' hb现货撤单 '''
        cancel_result, error_code = '', ''
        try:
            order_id = strategy_params.get('order_id', '')

            _logger.info(
                '----- hb_spot_cancel_order, order_id: {}, strategy_params: {} -----'.format(order_id, strategy_params))
            # 获取api配置参数
            api_dict = self.get_api_config_params(strategy_params)
            URL = api_dict.get('spot_url', '')  # 现货URL
            ACCESS_KEY = api_dict.get('access_key', '')
            SECRET_KEY = api_dict.get('secret_key', '')

            _logger.info(
                '----- hb_spot_cancel_order, api请求参数, ACCESS_KEY: {}, SECRET_KEY: {}, order_id: {} -----'.format(
                    ACCESS_KEY, SECRET_KEY, order_id))
            huobi_spot = HuobiSpot(URL, ACCESS_KEY, SECRET_KEY)
            api_result = huobi_spot.cancel_order(order_id=order_id)
            _logger.info('----- hb_spot_cancel_order, api返回结果, api_result: {} -----'.format(api_result))

            data = api_result.get('data', '')
            status = api_result.get('status', '')
            err_code = api_result.get('err-code', '')
            error_message = api_result.get('err-msg', '')

            trade_type = strategy_params.get('trade_type', '')
            entrust_price = strategy_params.get('entrust_price', '0')
            entrust_amount = strategy_params.get('entrust_amount', '0')
            part_deal_amount = strategy_params.get('part_deal_amount', '')
            if part_deal_amount:
                entrust_amount = float(entrust_amount) - float(part_deal_amount)

            if data and status.lower() != 'error' and err_code == '':
                # # ------------------------- 推送撤单成功日志 -------------------------------------
                # information = '{}，委托价格：{}，委托数量：{}，订单编号：{}'.format(trade_type, entrust_price, entrust_amount, order_id)
                # strategy_params['information'] = information
                # strategy_params['log_type'] = 'cancel'
                # self.push_log(strategy_params, 'log')
                # # ------------------------- 推送撤单成功日志 -------------------------------------

                self.delete_entrust_data(order_id)
                cancel_result = True
            else:
                # ------------------------- 推送撤单失败日志 -------------------------------------
                if not error_message: error_message = error_code
                information = '{}撤单失败，委托价格：{}，委托数量：{}，订单编号：{} [{}]'.format(trade_type, entrust_price, entrust_amount,
                                                                           order_id, error_message)
                strategy_params['information'] = information
                strategy_params['log_type'] = 'info'
                self.push_log(strategy_params, 'log')
                # ------------------------- 推送撤单失败日志 -------------------------------------

                cancel_result = False
        except Exception as e:
            _logger.error('----- hb_spot_cancel_order, {} -----'.format(traceback.format_exc()))
        finally:
            return cancel_result, error_code

    def ok_futures_cancel_order(self, strategy_params):
        ''' ok合约撤单 '''
        cancel_result, error_code = '', ''
        try:
            order_id = strategy_params.get('order_id', '')
            currency = strategy_params.get('currency', '')

            _logger.info(
                '----- ok_futures_cancel_order, order_id: {}, currency: {}, strategy_params: {} -----'.format(order_id,
                                                                                                              currency,
                                                                                                              strategy_params))
            # 获取api配置参数
            api_dict = self.get_api_config_params(strategy_params)
            access_key = api_dict.get('access_key', '')
            secret_key = api_dict.get('secret_key', '')
            pass_phrase = api_dict.get('pass_phrase', '')

            req = OkexRequest(api_key=access_key, api_seceret_key=secret_key, passphrase=pass_phrase,
                              use_server_time=True)
            okex_futures = OkexFutures(req)

            strategy_params['entrust_type'] = '合约'
            pair_id = self.get_pair_id(strategy_params)
            contract_date = self.verification.get_contract_date(pair_id)

            _instrument_id = '{}-{}-{}'.format(currency.upper(), 'USD', contract_date)

            _logger.info(
                '----- ok_futures_cancel_order, api请求参数, access_key: {}, secret_key: {}, pass_phrase: {}, instrument_id: {}, client_oid: {} -----'.format(
                    access_key, secret_key, pass_phrase, _instrument_id, order_id))
            api_result = okex_futures.cancel_orders(instrument_id=_instrument_id, client_oid=order_id)
            _logger.info('----- ok_futures_cancel_order, api_result: {} -----'.format(api_result))

            error_message = api_result.get('error_message', '')
            error_code = api_result.get('error_code', '')
            cancel_result = api_result.get('result', '')

            trade_type = strategy_params.get('trade_type', '')
            entrust_price = strategy_params.get('entrust_price', '0')
            entrust_amount = strategy_params.get('entrust_amount', '0')
            part_deal_amount = strategy_params.get('part_deal_amount', '')
            if part_deal_amount:
                entrust_amount = float(entrust_amount) - float(part_deal_amount)

            if cancel_result == True and str(error_code) == '0':
                # # ------------------------- 推送撤单成功日志 -------------------------------------
                # information = '{}，委托价格：{}，委托数量：{}，订单编号：{}'.format(trade_type, entrust_price, entrust_amount, order_id)
                # strategy_params['information'] = information
                # strategy_params['log_type'] = 'cancel'
                # self.push_log(strategy_params, 'log')
                # # ------------------------- 推送撤单成功日志 -------------------------------------

                self.delete_entrust_data(order_id)  # 撤单成功, 删委托
            else:
                # ------------------------- 推送撤单失败日志 -------------------------------------
                if not error_message: error_message = error_code
                information = '{}撤单失败，委托价格：{}，委托数量：{}，订单编号：{} [{}]'.format(trade_type, entrust_price, entrust_amount,
                                                                           order_id, error_message)
                strategy_params['information'] = information
                strategy_params['log_type'] = 'info'
                self.push_log(strategy_params, 'log')
                # ------------------------- 推送撤单失败日志 -------------------------------------

        except Exception as e:
            _logger.error('----- ok_futures_cancel_order, {} -----'.format(traceback.format_exc()))
        finally:
            return cancel_result, error_code

    def hb_futures_cancel_order(self, strategy_params):
        ''' hb合约撤单 '''
        cancel_result, error_code = '', ''
        try:
            order_id = strategy_params.get('order_id', '')
            currency = strategy_params.get('currency', '')

            _logger.info(
                '----- hb_futures_cancel_order, order_id: {}, strategy_params: {} -----'.format(order_id,
                                                                                                strategy_params))
            # 获取api配置参数
            api_dict = self.get_api_config_params(strategy_params)
            URL = api_dict.get('futures_url', '')  # 期货URL
            ACCESS_KEY = api_dict.get('access_key', '')
            SECRET_KEY = api_dict.get('secret_key', '')

            _logger.info(
                '----- hb_futures_cancel_order, api请求参数, ACCESS_KEY: {}, SECRET_KEY: {}, symbol: {}, order_id: {} -----'.format(
                    ACCESS_KEY, SECRET_KEY, currency.upper(), order_id))
            huobi_futures = HuobiFutures(URL, ACCESS_KEY, SECRET_KEY)
            api_result = huobi_futures.cancel_contract_order(symbol=currency.upper(), order_id=order_id)
            _logger.info('----- hb_futures_cancel_order, api返回结果, api_result: {} -----'.format(api_result))

            status = api_result.get('status', '')
            data = api_result.get('data', '')
            errors = data.get('errors', [])

            trade_type = strategy_params.get('trade_type', '')
            entrust_price = strategy_params.get('entrust_price', '0')
            entrust_amount = strategy_params.get('entrust_amount', '0')
            part_deal_amount = strategy_params.get('part_deal_amount', '')
            if part_deal_amount:
                entrust_amount = float(entrust_amount) - float(part_deal_amount)

            if status.lower() == 'ok' and not errors:
                # # ------------------------- 推送撤单成功日志 -------------------------------------
                # information = '{}，委托价格：{}，委托数量：{}，订单编号：{}'.format(trade_type, entrust_price, entrust_amount, order_id)
                # strategy_params['information'] = information
                # strategy_params['log_type'] = 'cancel'
                # self.push_log(strategy_params, 'log')
                # # ------------------------- 推送撤单成功日志 -------------------------------------

                self.delete_entrust_data(order_id)
                cancel_result = True
            else:
                errors = errors[0]
                error_code = errors.get('err_code', '')
                error_message = errors.get('err_msg', '')

                # ------------------------- 推送撤单失败日志 -------------------------------------
                if not error_message: error_message = error_code
                information = '{}撤单失败，委托价格：{}，委托数量：{}，订单编号：{} [{}]'.format(trade_type, entrust_price, entrust_amount,
                                                                           order_id, error_message)
                strategy_params['information'] = information
                strategy_params['log_type'] = 'info'
                self.push_log(strategy_params, 'log')
                # ------------------------- 推送撤单失败日志 -------------------------------------

                cancel_result = False
        except Exception as e:
            _logger.error('----- hb_futures_cancel_order, {} -----'.format(traceback.format_exc()))
        finally:
            return cancel_result, error_code

    def handler_entrust_price(self, entrust_price):
        ''' 处理委托价格 '''
        try:
            if str(entrust_price) in ['最新价', '市价', '对手价']:
                return entrust_price

            if entrust_price and float(entrust_price) == 0.0:
                entrust_price = '市价'
        except Exception as e:
            _logger.error('----- handler_entrust_price, {} -----'.format(traceback.format_exc()))
        finally:
            return entrust_price

    def get_operation_params(self, strategy_params: dict):
        ''' 封装操作字典 '''
        operation_time = int(time.time() * 1000000)
        operator_id, operator_name, operate_type, operate_page = strategy_params.get(
            'operator_id'), strategy_params.get(
            'operator_name'), strategy_params.get('operate_type'), strategy_params.get('operate_page')

        operation_dict = {
            'operator_id': operator_id,
            'operator_name': operator_name,
            'operation_time': operation_time,
            'pageName': operate_page,
            'type': operate_type
        }
        return {'sign': operation_dict}

    def futures_spot_save_operator_log(self, strategy_params, status):
        try:
            type = strategy_params.get('type', '')
            operate_number = 13 if type == 'buy' else 17
            remote_address = strategy_params.get('remote_address', '')
            operation_params = self.get_operation_params(strategy_params)

            _logger.info(
                '----- futures_spot_save_operator_log, operation_params: {}, remote_address: {}, strategy_params: {}, status: {}, operate_number: {} -----'.format(
                    operation_params, remote_address, strategy_params, status, operate_number))
            save_operator_log(operation_params, remote_address, operate_number, status)
        except Exception as e:
            _logger.error('----- futures_spot_save_operator_log, {} -----'.format(traceback.format_exc()))


if __name__ == '__main__':
    obj = Exchanges()
    params = {'type': 'sell', 'operator_id': '2c917d26757a046e01757a0750a50001', 'operator_name': 'ly',
              'operate_type': '卖出', 'operate_page': '期现持仓', 'remote_address': '183.49.47.85', 'sell_ratio': '100',
              'sell_type': '对手价', 'sell_amount': 3, 'currency': 'ETH', 'contract_size': 3, 'name': '火币网-ETH次季-ETH/USDT',
              'exchange': 'HB', 'id': 27, 'account_name': '阙尚钦Home', 'account_id': 'qsq', 'contract_type': 'bi_quarter',
              'buy_spot_amount': 0.0106, 'contract_positions_amount': 0.0,
              'information': '现货卖出，ETH，委托价格：市价，委托数量：0.0106', 'log_type': 'place', 'update_time': '2021-05-21 11:56:04',
              'entrust_type': '现货', 'trade_amount': '0.0106', 'trade_price': '0', 'order_id': 282249730663595}
