# -*- coding: UTF-8 -*-

import decimal
decimal.getcontext().rounding = "ROUND_HALF_UP"

import os
import time
import json
import threading
import traceback
from pathlib import Path
from pytz import timezone as timezoneBase
from datetime import datetime
from threading import Thread
import zmq

from utils.zmq_util import zmq_sub_asyn
from utils.log_util import logger
from utils.datetime_util import okex_contract_type
from utils.config import config as configBase
from utils.redis_util import redis_client, master_redis_client, lock_redis_client, trade_redis_client
from utils.mysql_util import mysql2redis, findAll
from utils.const import const
from utils.common_util import get_master_cache
from strategy_order.index import PlaceOrderMain
from utils.redis_lock import RedisLock
from utils.redis_sub_util import redis_psub_async
from strategy_order.base.send_email import SendOrderDetailEmail

base = PlaceOrderMain()
redis_lock = RedisLock(lock_redis_client)

class FuturesSpotGenerator:

    def __init__(self, client, topics, exchange=None):
        self.ppid = None
        self.client = client
        self.topics = topics
        self.exchange = exchange
        self.cal2storetopic = configBase.futures_spot__cal2storetopic if not exchange else "{}.{}".format(configBase.futures_spot__cal2storetopic, exchange)
        self.triggertopic = "futures.spot.strategy.trigger.queue" if not exchange else "futures.spot.strategy.trigger.queue.{}".format(exchange)
        self.EXCHANGE_CODE_MAPPING = {"HB": 1001, "OK": 1002}
        self.CODE_EXCHANGE_MAPPING = {1001: "HB", 1002: "OK"}
        self.CONTRACT_TYPE_CODE_MAPPING = {"this_week": "CW", const.CONTRACT_TYPE_NW: "NW",
                                      const.CONTRACT_TYPE_CQ: "CQ", const.CONTRACT_TYPE_NQ: "NQ"}
        self.CODE_CONTRACT_TYPE_MAPPING = {"CW": "this_week", "NW": const.CONTRACT_TYPE_NW,
                                      "CQ": const.CONTRACT_TYPE_CQ, "NQ": const.CONTRACT_TYPE_NQ}
        self.FUTURES_SPOT_PAIRS = {}
        self.RELATED_FUTURES_SPOT_PAIRS = {}
        # 期货现货对照
        self.CONTRACT_SPOT_MAPPING = {}
        # 期货现货明细信息
        self.CONTRACT_INFO_DICT = {}
        self.CONTRACT_TIMESTAMP_DICT = {"CONTRACT.TIMESTAMP": "-1"}
        self.QUOTION_CACHE = {}
        self.TIMESHARE_TIME_DICT = {}
        self.TIMESHARE_DATA_DICT = {}
        self.YESTERDAY = {}
        self.STRATEGY_PARAM_DICT = {}
        self.SYS_PARAM_DICT = {}

        context = zmq.Context()
        self.zmq_dealer = context.socket(zmq.DEALER)
        self.zmq_dealer.connect(configBase.strategy_comm_cal2storerouter)

        self._logger = logger("futures_spot_generator_log", "futures_spot_generator_log", configBase.futures_spot__logpath)
        self._info_logger = logger("futures_spot_generator_info", "futures_spot_generator_info", configBase.futures_spot__logpath)

        # self.CONTRACT_TIMESTAMP_DICT = {"CONTRACT.TIMESTAMP": "-1"}
        # self.QUOT__TIMESTAMP_DICT = {"TIMESTAMP": -1}
        self.SPOT_TOPICS_DICT = {'topics': None}
        self.CONTRACT_TIMESTAMP_DICT = -1
        self.CONTRACT_TIMESTAMP = -1
        self.QUOT__TIMESTAMP_DICT = -1
        self.QUOT__TIMESTAMP = -1

        self.send_email = SendOrderDetailEmail()

    def push_message(self, message, topic=None):
        # print("push_message:{}".format(message))
        msg = json.dumps(message)
        if not topic:
            topic = self.cal2storetopic
        redis_client.lpush(topic, msg)

    def get_spot_price(self, from_spot_pair, to_spot_pair=None):
        if not to_spot_pair:
            to_spot_pair = from_spot_pair

        if from_spot_pair == to_spot_pair:
            from_spot_value = to_spot_value = redis_client.get(from_spot_pair)
        else:
            from_spot_value, to_spot_value = redis_client.mget([from_spot_pair, to_spot_pair])

        if not from_spot_value and not to_spot_value:
            return None

        if not from_spot_value:
            from_spot_value = 99999999.99
        if not to_spot_value:
            to_spot_value = 99999999.99

        return min(float(from_spot_value), float(to_spot_value))

    def __notice(self, futures_spot_pair, estimated_annualized_yield):
        group_id = futures_spot_pair.get("id")
        key = "futures.spot.email.intervals.{}".format(group_id)
        if redis_client.get(key):
            return

        self.init_system_param()
        sys_param = self.SYS_PARAM_DICT.get("sys_param")
        emails = sys_param.get("futures_spot_notice_emails")
        if not emails:
            return
        emails = emails.split(";")
        intervals = int(sys_param.get("futures_spot_notice_intervals", 10))
        redis_client.setex(key, intervals * 60, 1)
        name = futures_spot_pair.get("name")
        msg = "期现套利组合{}当前预估年化收益率为：{}".format(name, estimated_annualized_yield)
        self.send_email.send_message(msg, emails, "期现策略：套利机会提醒")

    def __trigger__(self, futures_spot_pair, price, spot_price, diff, compare_rate, real_rate, estimated_annualized_yield, ts):
        # TODO:账户维度处理交易
        # if not self.STRATEGY_PARAM_DICT or not self.STRATEGY_PARAM_DICT.get("strategy_param"):
        #     return
        strategy_param = self.STRATEGY_PARAM_DICT.get("strategy_param")
        futures_spot_open_threshold = strategy_param.get("futures_spot_open_threshold", 20.0)

        if estimated_annualized_yield < futures_spot_open_threshold:
            return

        self.__notice(futures_spot_pair, estimated_annualized_yield)


    def __calculate_per_futures_spot_pair(self, futures_spot_pair, price, spot_price, ts):
        group_id = futures_spot_pair.get("id")
        contract = futures_spot_pair.get("contract")

        ch = futures_spot_pair.get("ch")
        face = futures_spot_pair.get("face")

        store_ts = redis_client.get("{}.trade.ts".format(contract))
        if store_ts and int(store_ts) > ts:
            # msg = "新来的合约1【{}】时间【{}】小于已存库的合约2【{}】时间【{}】，消息【{}】".format(from_contract, ts, to_contract, to_ts, message)
            # print(msg)
            # _logger.error(msg)
            return

        # 计算点差
        diff = decimal.Decimal(price) - decimal.Decimal(spot_price)
        diff = diff.quantize(decimal.Decimal("0.00"))
        real_rate = (decimal.Decimal(price) - decimal.Decimal(spot_price)) / decimal.Decimal(spot_price)

        compare_rate = real_rate * 100
        compare_rate = compare_rate.quantize(decimal.Decimal("0.00"))
        real_rate = real_rate.quantize(decimal.Decimal("0.0000"))

        delivery_time = futures_spot_pair.get("delivery_time")
        now_time = datetime.now(timezoneBase(configBase.timezone))
        date_diff = (delivery_time - now_time).days
        estimate_max_days = date_diff + 1
        estimated_annualized_yield = compare_rate * 365 / estimate_max_days
        estimated_annualized_yield = estimated_annualized_yield.quantize(decimal.Decimal("0.00"))

        Thread(target=self.__trigger__,
               args=(futures_spot_pair, price, spot_price, diff.__float__(), compare_rate.__float__(), real_rate.__float__(), estimated_annualized_yield.__float__(), ts,),
               name='__trigger__').start()
        msg = {"id": group_id, "d": diff.__float__(), "r": compare_rate.__float__(), "c": contract, "cp": price,
                      "sp": spot_price, "cc": ch, "face": face, "days": estimate_max_days, "yield": estimated_annualized_yield.__float__(), "ts": ts}
        # Thread(target=push_message, args=(message1,)).start()
        self.push_message(msg)

    def calculate(self, futures_spot_pair, message):
        contract_code = futures_spot_pair.get("contract")
        message["contract_code"] = contract_code
        ch = message.get(const.TAG_PAIR)
        ts = message.get(const.TAG_UPDATETIME)
        sendtime = message.get(const.TAG_SENDTIME)

        # 保存最新的行情交易数据
        # TODO：如果新到信息的ts比存库的ts还要小，那直接扔掉
        key_ts = "{}.trade.ts".format(contract_code)
        spot_ch = self.CONTRACT_SPOT_MAPPING.get(ch)
        store_ts, spot_value = redis_client.mget([key_ts, spot_ch])
        if store_ts and int(store_ts) > ts:
            # msg = "新来的消息【{}】时间小于已存库时间【{}】".format(message, store_ts)
            # print(msg)
            # _logger.error(msg)
            return
        # redis_client.set(key_ts, ts)
        price = message.get(const.TAG_PRICE, None)
        key_price = "{}.trade.price".format(contract_code)
        # redis_client.set(key_price, price)
        redis_client.mset({key_ts: ts, key_price: price})
        redis_client.expire(key_price, configBase.contract_timeout)

        if not spot_value:
            # TODO: 2021-03-09不存合约行情数据
            # self.push_message(message)
            error = "===>获取不到现货【{}】数据。。。".format(spot_ch)
            print(error)
            # _logger.error(error)
            return

        spot_price = float(spot_value)
        Thread(target=self.__calculate_per_futures_spot_pair, args=(futures_spot_pair, price, spot_price, ts,), name='__calculate_per_futures_spot_pair').start()

    def __filterMessage(self, message, client=None):
        ch = message.get(const.TAG_PAIR)
        if ch in [1001000001, 1001000004, 1001000005, 1002000021, 1002000024, 1002000025]:
            return True
        recvtime = int(time.time() * 1000000)
        updatetime = message.get(const.TAG_UPDATETIME)
        # msg = "======recvtime：{}，updatetime：{}，间隔：{}".format(recvtime, updatetime, recvtime - updatetime)
        # print(msg)
        # self._info_logger.info(msg)
        # if recvtime - updatetime < -500000:
        #     pid = os.getpid()
        #     msg = "{}消息接收延迟超过60s：{}；接收时间：{}".format(self.client, message, recvtime)
        #     self.send_email.send_message(msg, ['wanglili@wescxx.com'], '行情接收延迟')
        #     print(msg)
        #     self._info_logger.warning(msg)
        #     os.kill(pid, 9)
        if recvtime - updatetime > 60000000:
            contract_pairs = self.CONTRACT_PAIRS.get("CONTRACT_PAIRS")
            contract_code = self.get_contract_code(ch)
            related_contract_pairs = self.get_related_contract_pairs(contract_pairs, contract_code)
            if not related_contract_pairs:
                return True
            pid = os.getpid()
            msg = "{}消息接收延迟超过60s：{}；接收时间：{}".format(self.client, message, recvtime)
            print(msg)
            self._logger.warning(msg)
            self.init_system_param()
            sys_param = self.SYS_PARAM_DICT.get("sys_param")
            emails = sys_param.get("maintenance_email")
            if emails:
                emails = emails.split(";")
                if self.ppid:
                    title =  '{}行情接收延迟，建议重启futures_spot_starter重新订阅'.format(self.client)
                else:
                    title = '{}行情接收延迟，自动停止订阅'.format(self.client)
                self.send_email.send_message(msg, emails, title)
            if not self.ppid:
                os.kill(pid, 9)
        sendtime = message.get(const.TAG_SENDTIME)
        if sendtime - updatetime > 1000000:
            # print("=======sendtime：【{}】超过updatetime：【{}】{}秒，【{}】".format(sendtime, updatetime, interval, message))
            return True
        """
                if self.QUOT__TIMESTAMP_DICT.get("TIMESTAMP") > updatetime:
            # print("=======消息【{}】到的比【{}】晚".format(updatetime, self.QUOT__TIMESTAMP_DICT.get("TIMESTAMP")))
            return True
        self.QUOT__TIMESTAMP_DICT["TIMESTAMP"] = updatetime

        ch_ts = ".".join([ch, updatetime])
        if self.CONTRACT_TIMESTAMP_DICT.get("CONTRACT.TIMESTAMP") == ch_ts:
            # print("{}.{}已接收".format(ch, updatetime))
            return True
        self.CONTRACT_TIMESTAMP_DICT["CONTRACT.TIMESTAMP"] = ch_ts
        return False
        """
        if self.QUOT__TIMESTAMP > updatetime:
            # print("=======消息【{}】到的比【{}】晚".format(updatetime, self.QUOT__TIMESTAMP_DICT))
            return True
        self.QUOT__TIMESTAMP = updatetime

        ch_ts = "{}.{}".format(ch, updatetime)
        if self.CONTRACT_TIMESTAMP == ch_ts:
            # print("{}.{}已接收".format(ch, updatetime))
            return True
        self.CONTRACT_TIMESTAMP = ch_ts
        return False

    def __handleMessage(self, message, client=None):
        ch = message.get(const.TAG_PAIR)
        futures_spot_pairs = self.FUTURES_SPOT_PAIRS.get("FUTURES_SPOT_PAIRS")
        futures_spot_pair_dict = self.FUTURES_SPOT_PAIRS.get("FUTURES_SPOT_PAIR_DICT")
        if not futures_spot_pairs:
            return
        try:
            # contract_code = self.get_contract_code(ch)
            # if not contract_code:
            #     return
            futures_spot_pair = futures_spot_pair_dict.get(ch)
            if not futures_spot_pair:
                return
            if self.__filterMessage(message, client):
                return
            # length = len(threading.enumerate())
            # active_count = threading.active_count()
            # print('运行：【{}】，激活：【{}】'.format(length, active_count))
            Thread(target=self.calculate, args=(futures_spot_pair, message,), name='calculate').start()
            return
        except Exception as e:
            msg = traceback.format_exc()
            print(msg)
            self._logger.error(msg)

    def init_contract_info(self):
        redis_key = const.MARKET_VARIETY_INFO
        infos = master_redis_client.lrange(redis_key, 0, -1)
        friday_dict = okex_contract_type()
        for info in infos:
            info = json.loads(info)
            if info.get("goods_type") != 2:
                continue
            id = info.get("id")
            exchange = info.get("exchange_id")
            underlying_id = info.get("underlying_id")
            sub_symbol = info.get("sub_symbol")
            instrument_id = info.get('instrument_id')

            currency = instrument_id[0:3]
            contract_type = friday_dict.get(instrument_id[-6:])

            # ss = instrument_id.split("_")
            # if len(ss) == 1:
            #     currency = instrument_id[0:-6]
            #     contract_type = friday_dict.get(instrument_id[-6:])
            # else:
            #     ss = sub_symbol.split("-")
            #     currency = ss[0]
            #     contract_type = friday_dict.get(ss[-1])
            # ss = sub_symbol.split("_")
            # if len(ss) == 2:
            #     currency = ss[0]
            #     contract_type = ss[-1]
            # else:
            #     ss = sub_symbol.split("-")
            #     currency = ss[0]
            #     contract_type = friday_dict.get(ss[-1])

            if not currency or not contract_type:
                continue

            # info[const.CONTRACT_CODE] = '{}.{}.{}'.format(CODE_EXCHANGE_MAPPING.get(exchange), currency, CODE_CONTRACT_TYPE_MAPPING.get(contract_type))
            contract_code = '{}.{}.{}'.format(self.CODE_EXCHANGE_MAPPING.get(exchange), currency, contract_type)
            ss = instrument_id.split("-")
            if len(ss) == 3 and const.DEPOSIT_TYPE_USD != ss[1]:
                contract_code = '{}.{}'.format(contract_code, ss[1])
            info["contract_code"] = contract_code
            if info.get("delivery_time"):
                info["delivery_time_utc8"] = datetime.fromtimestamp(info.get("delivery_time") / 1000, tz=timezoneBase(configBase.timezone))


            self.CONTRACT_INFO_DICT[id] = info
            for info1 in infos:
                info1 = json.loads(info1)
                if info1.get("goods_type") != 1:
                    continue
                if exchange == info1.get("exchange_id") and underlying_id == info1.get("underlying_id"):
                    self.CONTRACT_SPOT_MAPPING[info.get("id")] = info1.get("id")
                    break

    def init_futures_spot_pairs(self):
        # TODO:此处增加合约对数据初始化功能，初始从redis取即可,后续更新订阅消息来实现
        # this_week_friday, next_week_friday, this_quarter_end_friday, next_quarter_end_friday = fridays()
        friday_dict = okex_contract_type()
        value = get_master_cache(const.FUTURES_SPOT_PAIRS)
        futures_spot_pair_dict = {}
        if value:
            # global FUTURES_SPOT_PAIRS
            futures_spot_pairs = json.loads(value)
            if isinstance(futures_spot_pairs, str):
                futures_spot_pairs = json.loads(futures_spot_pairs)
            # 找到对应的pair
            for futures_spot_pair in futures_spot_pairs:
                exchange1 = futures_spot_pair.get("exchange")
                currency1 = futures_spot_pair.get("currency")
                contract_type1 = futures_spot_pair.get("contract_type")
                contract = "{}.{}.{}".format(exchange1, currency1, contract_type1)
                futures_spot_pair["contract"] = contract
                deposit = const.DEPOSIT_TYPE_USD

                for key, contract_info in self.CONTRACT_INFO_DICT.items():
                    if contract_info.get("goods_type") == 1 or contract_info.get("currency_id") == 13:
                        continue

                    exchange = contract_info.get("exchange_id")
                    instrument_id = contract_info.get('instrument_id')
                    currency = instrument_id[0:3]
                    contract_type = friday_dict.get(instrument_id[-6:])

                    if not currency or not contract_type:
                        continue

                    if exchange in [exchange1, self.EXCHANGE_CODE_MAPPING.get(exchange1)] and currency in [currency1] and contract_type in [contract_type1, self.CONTRACT_TYPE_CODE_MAPPING.get(contract_type1)]:
                        ch = contract_info.get("id")
                        face = contract_info.get("contract_unit")
                        price_tick = contract_info.get("price_tick")
                        delivery_time = contract_info.get("delivery_time_utc8")

                        futures_spot_pair["ch"] = ch
                        futures_spot_pair["face"] = face
                        futures_spot_pair["price_tick"] = price_tick
                        futures_spot_pair["spot"] = self.CONTRACT_SPOT_MAPPING.get(ch)
                        futures_spot_pair["delivery_time"] = delivery_time
                        futures_spot_pair_dict[ch] = futures_spot_pair
            # store contract pair info to master redis
            # master_redis_client.set(const.FUTURE_SPOT_PAIRS_INFO, json.dumps(futures_spot_pairs, ensure_ascii=False))
            return futures_spot_pairs, futures_spot_pair_dict
        return [], {}

    def init_strategy_param(self):
        value = get_master_cache(const.STRATEGY_PARAM)
        strategy_param_dict = json.loads(value)
        if isinstance(strategy_param_dict, str):
            strategy_param_dict = json.loads(strategy_param_dict)
        strategy_param_dict["auto_build_repo"] = bool(strategy_param_dict.get("auto_build_repo"))
        strategy_param_dict["auto_close_repo"] = bool(strategy_param_dict.get("auto_close_repo"))
        return strategy_param_dict

    def init_system_param(self):
        value = get_master_cache(const.SYS_PARAM)
        sys_param_dict = json.loads(value)
        if isinstance(sys_param_dict, str):
            sys_param_dict = json.loads(sys_param_dict)

        self.SYS_PARAM_DICT["sys_param"] = sys_param_dict
        return sys_param_dict

    def __listen_strategy_param_queue(self):
        """
        监听合约对消息，有消息则刷新
        :return:
        """
        pubsub = master_redis_client.pubsub()
        pubsub.subscribe(const.STRATEGY_PARAM_QUEUE)
        while True:
            print("__listen_strategy_param_queue")
            self._logger.info("__listen_strategy_param_queue")
            queue_item = pubsub.parse_response(block = True)
            if queue_item:
                if isinstance(queue_item[-1],int):
                    continue
                try:
                    value = queue_item[-1]
                    strategy_param_dict = json.loads(value)
                    if isinstance(strategy_param_dict, str):
                        strategy_param_dict = json.loads(strategy_param_dict)
                    strategy_param_dict["auto_build_repo"]=bool(strategy_param_dict.get("auto_build_repo"))
                    strategy_param_dict["auto_close_repo"] = bool(strategy_param_dict.get("auto_close_repo"))
                    self.STRATEGY_PARAM_DICT["strategy_param"] = strategy_param_dict
                    # STRATEGY_PARAM_DICT = init_strategy_param()
                    msg = "===============策略参数变更【{}】================".format(self.STRATEGY_PARAM_DICT)
                    # print(msg)
                    self._logger.info(msg)
                    message = {}
                    message["updatetime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    message["data"] = json.loads(queue_item[-1])
                    master_redis_client.lpush("strategy.param.queue.history", json.dumps(message))
                except Exception as e:
                    pass

    def __listen_futures_spot_pairs_queue(self):
        """
        监听合约对消息，有消息则刷新
        :return:
        """
        pubsub = master_redis_client.pubsub()
        pubsub.subscribe(const.FUTURES_SPOT_PAIRS_QUEUE)
        while True:
            # print("__listen_futures_spot_pairs_queue")
            self._logger.info("__listen_futures_spot_pairs_queue")
            queue_item = pubsub.parse_response(block = True)
            if queue_item:
                if isinstance(queue_item[-1],int):
                    continue
                try:

                    global RELATED_FUTURES_SPOT_PAIRS
                    RELATED_FUTURES_SPOT_PAIRS = {}
                    mysql2redis()
                    self.init_contract_info()
                    futures_spot_pairs, futures_spot_pair_dict = self.init_futures_spot_pairs()
                    self.FUTURES_SPOT_PAIRS["FUTURES_SPOT_PAIRS"] = futures_spot_pairs
                    self.FUTURES_SPOT_PAIRS["FUTURES_SPOT_PAIR_DICT"] = futures_spot_pair_dict
                    msg = "===============期现组合变更【{}】，交易订阅行情明细【{}】================".format(futures_spot_pairs, self.CONTRACT_INFO_DICT)
                    self._logger.info(msg)
                    message = {}
                    message["updatetime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    message["data"] = json.loads(queue_item[-1])
                    master_redis_client.lpush("contract.pairs.queue.history", json.dumps(message))
                except Exception as e:
                    pass

    def __listen_quotion_topics_queue(self):
        """
        监听行情主题变更消息，有消息则刷新
        :return:
        """
        pubsub = master_redis_client.pubsub()
        pubsub.subscribe(const.QUOTION_CONTRACT_TOPICS_QUEUE)
        while True:
            queue_item = pubsub.parse_response(block=True, timeout=10)
            if queue_item:
                if isinstance(queue_item[-1], int):
                    continue
                try:
                    global RELATED_FUTURES_SPOT_PAIRS
                    RELATED_FUTURES_SPOT_PAIRS = {}
                    mysql2redis()
                    self.init_contract_info()
                    futures_spot_pairs, futures_spot_pair_dict = self.init_futures_spot_pairs()
                    self.FUTURES_SPOT_PAIRS["FUTURES_SPOT_PAIRS"] = futures_spot_pairs
                    self.FUTURES_SPOT_PAIRS["FUTURES_SPOT_PAIR_DICT"] = futures_spot_pair_dict
                    msg = "===============交易所行情主题变更【{}】，交易订阅行情明细【{}】================".format(futures_spot_pairs, self.CONTRACT_INFO_DICT)
                    self._logger.info(msg)
                except Exception as e:
                    pass

    def __listen_spot_quotion_topics_queue(self):
        """
        监听行情主题变更消息，有消息则刷新
        :return:
        """
        pubsub = master_redis_client.pubsub()
        pubsub.subscribe(const.QUOTION_SPOT_TOPICS_QUEUE)
        while True:
            queue_item = pubsub.parse_response(block=True, timeout=10)
            if queue_item:
                if isinstance(queue_item[-1], int):
                    continue
                try:
                    self.SPOT_TOPICS_DICT['topics'] = []
                    topics = json.loads(queue_item[-1]).get("topics")
                    spot_topics = set()
                    for topic in topics:
                        if 2 == int(topic[1]):
                            continue
                        spot_topics.add(topic[0])
                        self.SPOT_TOPICS_DICT['topics'] = spot_topics
                    self._logger.info("===============行情主题变更，监听频道【{}】================".format(spot_topics))
                    mysql2redis()
                except Exception as e:
                    pass

    def init_spot_topics(self):
        topics = findAll(1)
        contract_topics = set()
        for topic in topics:
            if "2" == topic[1]:
                continue
            contract_topics.add(topic[0])
        self.SPOT_TOPICS_DICT['topics'] = contract_topics

    def __listen_contract_quot(self, queue):
        """
        监听合约对消息，有消息则刷新
        :return:
        """
        pubsub = trade_redis_client.pubsub()
        pubsub.psubscribe(queue)
        while True:
            queue_item = pubsub.parse_response(block = True)
            if queue_item:
                if isinstance(queue_item[-1],int):
                    continue
                try:
                    data = json.loads(queue_item[-1])
                    self.__handleMessage(data)
                except Exception as e:
                    pass

    def start(self):
        pid = os.getpid()
        ppid = os.getppid()
        print("==============>pid:{}, ppid:{}".format(pid, ppid))
        try:
            mysql2redis()
            self.init_contract_info()
            futures_spot_pairs, futures_spot_pair_dict = self.init_futures_spot_pairs()
            self.FUTURES_SPOT_PAIRS["FUTURES_SPOT_PAIRS"] = futures_spot_pairs
            self.FUTURES_SPOT_PAIRS["FUTURES_SPOT_PAIR_DICT"] = futures_spot_pair_dict

            strategy_param_dict = self.init_strategy_param()
            self.STRATEGY_PARAM_DICT["strategy_param"] = strategy_param_dict

            sys_param = self.init_system_param()

            Thread(target=self.__listen_futures_spot_pairs_queue, args=(), name='__listen_futures_spot_pairs_queue').start()
            Thread(target=self.__listen_strategy_param_queue, args=(), name='__listen_strategy_param_queue').start()
            Thread(target=self.__listen_quotion_topics_queue, args=(), name='__listen_quotion_topics_queue').start()

            self.init_spot_topics()
            Thread(target=self.__listen_spot_quotion_topics_queue, args=(), name='__listen_spot_quotion_topics_queue').start()

            msg = "{}启动完成".format(self.client)
            print(msg)
            self._logger.info(msg)
            # TODO:这里默认pull了hb和ok两个push的数据
            # zmq_pull(urls=configBase.strategy_comm_pushurls, timeout=60000, callback=self.__handleMessage, asynccall=False, client=client, printmsg=False)
            port = configBase.zmq_tradeport
            url = configBase.zmq_host.format(port)
            timeout = configBase.zmq_timeout

            commMode = configBase.commMode
            if const.COMM_MODE_REDIS == commMode:
                passwod = configBase.redis_trade_password
                if not passwod:
                    url = "redis://{}:{}/{}".format(configBase.redis_trade_host, configBase.redis_trade_port, configBase.redis_trade_db)
                else:
                    url = "redis://:{}@{}:{}/{}".format(configBase.redis_trade_password, configBase.redis_trade_host,
                                                        configBase.redis_trade_port, configBase.redis_trade_db)
                redis_psub_async(url=url, topic=self.topics, callback=self.__handleMessage, client=self.client)
                # # self.__listen_contract_quot(self.topics)
                # loop = asyncio.get_event_loop()
                # # tasks = [self.__listen_contract_quot(url, self.topics), self.__listen_contract_quot(url, "trade.1001*"), ]
                # tasks = [_psub(url,  "trade.1001*", self.__handleMessage, self.client),
                #          _psub(url, "trade.1002*", self.__handleMessage, self.client), ]
                # loop.run_until_complete(asyncio.gather(asyncio.gather(*(task for task in tasks), loop=loop)))
            else:
                zmq_sub_asyn(url, self.topics, timeout, callback=self.__handleMessage, client=self.client, printmsg=False)
        except Exception as e:
            msg = traceback.format_exc()
            print(msg)
            self._logger.error(msg)
        finally:
            os.kill(pid, 9)

def hb(ppid=None):
    print('Parent process %s.' % os.getpid())
    topics = "trade.1001*"
    exchange = "hb"
    client = "{}.{}".format(Path(__file__).stem, exchange)
    hb_generator = FuturesSpotGenerator(client=client, topics=topics, exchange=exchange)
    if ppid:
        hb_generator.ppid = ppid
    hb_generator.start()

def ok(ppid=None):
    print('Parent process %s.' % os.getpid())
    topics = "trade.1002*"
    exchange = "ok"
    client = "{}.{}".format(Path(__file__).stem, exchange)
    ok_generator = FuturesSpotGenerator(client=client, topics=topics, exchange=exchange)
    if ppid:
        ok_generator.ppid = ppid
    ok_generator.start()

if __name__ == '__main__':
    hb()