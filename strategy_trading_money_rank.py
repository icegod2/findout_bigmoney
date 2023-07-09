import pandas as pd
import time
import datetime
import queue
import numpy as np
import json
import os
import subprocess
import errno
import itertools
import shutil

class Trading_money_rank:
    # 建構式rank_up_day_cnt
    def __init__(self):
        self.cumulative_rank_up_day = 5 
        self.selldout_days_after_buy = 10
        # self.trading_money_min = 300000000
        self.trading_money_min = 0
        self.collect_data_start_date = "2010-01-01"
        self.collect_data_end_date = "2021-12-31"
        self.valid_start_date = "2022-1-01"
        self.valid_end_date = "2023-12-31"
        self.not_meet_up_order_criteria = 1
        self.data_fn_fmt = "{}{}_{}"
        self.stock_data_dir = "./db/"
        self.debug_stock_select_num = 3
        self.max_record_need_each_stock = 3
        self.stocklist_fn = "./stocklist.csv"
        self.buy_rule_fn = "./buy_rule_by_trading_money_rank_strategy"
        self.data_dir = "./strategy_trading_money_rank_data/"
        # self.test_case = [  ["2006-01-01", "2016-12-31", "2017-01-01", "2023-06-30"],
        #                     ["2008-01-01", "2018-12-31", "2019-01-01", "2023-06-30"],
        #                     ["2010-01-01", "2020-12-31", "2021-01-01", "2023-06-30"],
        #                     ["2012-01-01", "2021-12-31", "2022-01-01", "2023-06-30"],
        #                     ["2014-01-01", "2021-12-31", "2022-01-01", "2023-06-30"],
        #                     ["2016-01-01", "2021-12-31", "2022-01-01", "2023-06-30"],
        #                     ["2018-01-01", "2021-12-31", "2022-01-01", "2023-06-30"],
        #                     ["2020-01-01", "2021-12-31", "2022-01-01", "2023-06-30"],
        #                 ]  

        self.test_case = [  ["2006-01-01", "2016-12-31", "2017-01-01", "2023-06-30"]
                        ]  
        
    def set_collect_data_start_date(self, day):
        self.collect_data_start_date = day

    def set_collect_data_end_date(self, day):
        self.collect_data_end_date = day

    def set_valid_start_date(self, day):
        self.valid_start_date = day
    
    def set_valid_end_date(self, day):
        self.valid_end_date = day
  
    def set_data_fn_fmt(self, fmt):
        self.data_fn_fmt = fmt

    def set_stock_data_dir(self, path):
        self.stock_data_dir = path

    def set_cumulative_rank_up_day(self, val):
        self.cumulative_rank_up_day = val

    def set_selldout_days_after_buy(self, val):
        self.selldout_days_after_buy = val
    
    def get_collect_data_start_date(self):
        return self.collect_data_start_date
    
    def get_collect_data_end_date(self):
        return self.collect_data_end_date
    
    def get_valid_start_date(self):
        return self.valid_start_date
    
    def get_valid_end_date(self):
        return self.valid_end_date

    def get_cumulative_rank_up_day(self):
        return self.cumulative_rank_up_day

    def get_selldout_days_after_buy(self):
        return self.selldout_days_after_buy

    def _check_if_time_to_buy(self, stock_id, stock_name, cumulative_rank_up_day):
        fn = self.data_fn_fmt.format(self.stock_data_dir, stock_id, stock_name)
        stock_data = pd.read_csv(fn)
        last_trading_money_rank = 0
        for index, row in stock_data.tail(cumulative_rank_up_day + 1).iterrows():
            check_date = row[0]
            close_price = row[7]
            trading_money_rank = row[10]
            
            print(check_date, close_price, trading_money_rank)

            if trading_money_rank == -1:
                continue

            if last_trading_money_rank == 0:
                last_trading_money_rank = trading_money_rank
                continue

            if trading_money_rank > last_trading_money_rank:
                return 0
        return 1


    def pick_up_stock(self):
        stocklist = pd.read_csv(self.stocklist_fn)
        inform_use = []
        for index, row in stocklist.head.iterrows():
            curr_stock_id = int(row['有價證券代號'])
            curr_stock_name = row['有價證券名稱']
            cumulative_rank_up_day = 0
            selldout_days_after_buy = 0
            print("checking {} {}".format(curr_stock_id, curr_stock_name))
            cmd = 'grep "^{}:" {} | cut -d ":" -f 3'.format(curr_stock_id, self.buy_rule_fn)
            cumulative_rank_up_day = subprocess.getoutput(cmd)
            if len(cumulative_rank_up_day) == 0:
                continue
            cumulative_rank_up_day = int(cumulative_rank_up_day)
            cmd = 'grep "^{}:" {} | cut -d ":" -f 4'.format(curr_stock_id, self.buy_rule_fn)
            selldout_days_after_buy = subprocess.getoutput(cmd)
            if len(selldout_days_after_buy) == 0:
                continue
            selldout_days_after_buy = int(selldout_days_after_buy)

            print(cumulative_rank_up_day, selldout_days_after_buy)
            ret = self._check_if_time_to_buy(curr_stock_id, curr_stock_name, cumulative_rank_up_day)
            if ret == 1:
                s = "[{} {}][{} {}] buy".format(curr_stock_id, curr_stock_name, cumulative_rank_up_day, selldout_days_after_buy)
                inform_use.append(s)
        return inform_use


    def update_buy_criteria(self):
        stocklist = pd.read_csv(self.stocklist_fn)
        round = len(self.test_case)
        try:
            os.remove(self.buy_rule_fn)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise
            else:
                pass

        for index, row in stocklist.head(self.debug_stock_select_num).iterrows():
            curr_stock_id = int(row['有價證券代號'])
            curr_stock_name = row['有價證券名稱']
            # rank_set = set()
            max_cnt = int(round / 2)
            max_cnt_result = ""
            for cumulative_rank_up_day in range(2, 10):
                for selldout_days_after_buy in range(10, 61, 10):
                    cmd = 'grep -r "\[{} " {} | grep "\[{} {}\]" | wc -l'.format(curr_stock_id, self.data_dir, cumulative_rank_up_day, selldout_days_after_buy)
                    output = subprocess.getoutput(cmd)
                    if int(output) > 0:
                        result = "{}:{}:{}:{}:{}:{}".format(curr_stock_id, curr_stock_name, cumulative_rank_up_day, selldout_days_after_buy, output, round)
                        # print(result)
                        if int(output) >= max_cnt:
                            max_cnt = int(output)
                            max_cnt_result = result
            if len(max_cnt_result) > 0:
                cmd = 'echo "{}" >> {}'.format(max_cnt_result, self.buy_rule_fn)
                os.system(cmd)

    def update_data_start(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)
        os.mkdir(self.data_dir)
        for m in self.test_case:
            self._update_data_sub(m)

    def _update_data_sub(self, date_array):
        print(self.collect_data_start_date)
        print(self.collect_data_end_date)
        self.set_collect_data_start_date(date_array[0])
        self.set_collect_data_end_date(date_array[1])
        self.set_valid_start_date(date_array[2])
        self.set_valid_end_date(date_array[3])
        strategy_dict = dict()
        stocklist = pd.read_csv(self.stocklist_fn)
        fn = "{}{}_{}_{}_{}.log".format(self.data_dir, self.get_collect_data_start_date(), self.get_collect_data_end_date(), self.get_valid_start_date(), self.get_valid_end_date())
        with open(fn, 'w+') as file:
            file = open(fn, "w+")
            file.write("Train: {} {}\n".format(self.get_collect_data_start_date(), self.get_collect_data_end_date()))
            file.write("Valid: {} {}\n".format(self.get_valid_start_date(), self.get_valid_end_date()))
            for index, row in stocklist.head(self.debug_stock_select_num).iterrows():
                curr_stock_id = int(row['有價證券代號'])
                curr_stock_name = row['有價證券名稱']
                return_rate_dic = dict()
                print(curr_stock_id, curr_stock_name)
                for cumulative_rank_up_day in range(2, 3):
                    self.not_meet_up_order_criteria = 1
                    for selldout_days_after_buy in range(10, 31, 10):
                        # print("[{} {}]================= cumulative_rank_up_day => {}".format(curr_stock_id, curr_stock_name, cumulative_rank_up_day))
                        # print("[{} {}]================== selldout_days_after_buy => {}".format(curr_stock_id, curr_stock_name, selldout_days_after_buy))
                        self.set_cumulative_rank_up_day(cumulative_rank_up_day)
                        self.set_selldout_days_after_buy(selldout_days_after_buy)
                        j_ret= self._collect_data(curr_stock_id, curr_stock_name) 
                        if j_ret == None:
                            continue
                        j_ret=json.loads(j_ret)
                        r = round(j_ret['return_rate'], 5)
                        s_return_rate = str(r)
                        if s_return_rate in return_rate_dic:
                            return_rate_dic[s_return_rate].append(j_ret)
                        else:
                            return_rate_dic[s_return_rate] = [j_ret]
                    if self.not_meet_up_order_criteria == 1:
                        break

                if len(return_rate_dic) > 0:
                    return_rate_dic = dict(sorted(return_rate_dic.items(), reverse=True))
                    return_rate_dic = dict(itertools.islice(return_rate_dic.items(), 0, self.max_record_need_each_stock))
                    cnt = self.max_record_need_each_stock
                    for key in return_rate_dic:
                        for j_ret in return_rate_dic[key]:
                            # print(j_ret)
                            self.set_cumulative_rank_up_day(j_ret['cumulative_rank_up_day'])
                            self.set_selldout_days_after_buy(j_ret['selldout_days_after_buy'])
                            collect_data_return_rate = 100 * round(j_ret['return_rate'], 3)
                            if collect_data_return_rate < 1:
                                continue;
                            collect_data_period = 100 * round(j_ret['period'], 1)
                            j_ret= self._collect_data_for_valid_date(curr_stock_id, curr_stock_name) 
                            if j_ret == None:
                                continue
                            j_ret=json.loads(j_ret)
                            valid_return_rate = 100 * round(j_ret['return_rate'], 3)
                            valid_period = 100 * round(j_ret['period'], 1)
                            delta_return_rate= valid_return_rate - collect_data_return_rate
                            if valid_return_rate > 0:
                                file.write("[{} {}][{} {}] return rate: train: {:.2f}%, valid: {:.2f}%, delta: {:.2f} (period {:.2f} {:.2f})\n".format(curr_stock_id, curr_stock_name, j_ret['cumulative_rank_up_day'], j_ret['selldout_days_after_buy'], collect_data_return_rate, valid_return_rate, delta_return_rate, collect_data_period, valid_period))
                                # print("[{} {}][{} {}] return rate: train: {:.2f}%, valid: {:.2f}%, delta: {:.2f} (period {:.2f} {:.2f})".format(curr_stock_id, curr_stock_name, j_ret['cumulative_rank_up_day'], j_ret['selldout_days_after_buy'], collect_data_return_rate, valid_return_rate, delta_return_rate, collect_data_period, valid_period))


    def _collect_data_for_valid_date(self, stock_id, stock_name):
        fn = self.data_fn_fmt.format(self.stock_data_dir, stock_id, stock_name)
        stock_data = pd.read_csv(fn)
        init_money = 10000
        final_money = 10000

        start_epoch=0
        end_epoch=0
        detail_list = []
        element = datetime.datetime.strptime(self.valid_start_date,"%Y-%m-%d")
        tuple = element.timetuple()
        start_epoch = time.mktime(tuple)
        
        element = datetime.datetime.strptime(self.valid_end_date,"%Y-%m-%d")
        tuple = element.timetuple()
        end_epoch = time.mktime(tuple)

        rank_list = []
        buy_price = 0
        buy_date = 0
        sell_date = 0
        sell_price = 0
        last_sell_epoch = 0
        status = 0  # 1: have stock

        for index, row in stock_data.iterrows():
            check_date = row[0]
            close_price = row[7]
            trading_money_rank = row[10]

            if close_price == 0:
                continue

            element = datetime.datetime.strptime(check_date,"%Y-%m-%d")
            tuple = element.timetuple()
            epoch = time.mktime(tuple)
            if epoch >= start_epoch and epoch <= end_epoch:
                # print("checkdate {}".format(check_date))
                if epoch <= last_sell_epoch:
                    # print("epoch {}, last_sell_epoch {}".format(epoch, last_sell_epoch))
                    continue
                    
                if len(rank_list) > self.cumulative_rank_up_day:
                    up_order = 0
                    last_rank = rank_list[0]
                    for i in range(1, len(rank_list)):
                        if last_rank >= rank_list[i]:
                            up_order += 1
                            last_rank = rank_list[i]

                    if up_order >= self.cumulative_rank_up_day:
                        # print("check_date {}".format(check_date))
                        buy_price = row[7]
                        buy_date = check_date

                        element = datetime.datetime.strptime(buy_date,"%Y-%m-%d")
                        tuple = element.timetuple()
                        buy_epoch = time.mktime(tuple)
                        sell_epoch_start = buy_epoch + self.selldout_days_after_buy * 86400
                        sell_epoch_end = buy_epoch + (self.selldout_days_after_buy * 3) * 86400

                        # print("sell_epoch_start {}, sell_epoch_end {}".format(sell_epoch_start, sell_epoch_end))
                        # print("buy at {} at price {}, buy_epoch {}".format(buy_date, buy_price, buy_epoch))
                        for sell_index, sell_row in stock_data.iloc[index + self.selldout_days_after_buy:].iterrows():
                            # print(sell_row)
                            sell_date = sell_row[0]
                            sell_price = sell_row[7]
                            
                            element = datetime.datetime.strptime(sell_date,"%Y-%m-%d")
                            tuple = element.timetuple()
                            sell_epoch = time.mktime(tuple)
                            if sell_epoch >= sell_epoch_start and sell_epoch < sell_epoch_end:
                                if sell_price > 0:
                                    # print("criteria ", criteria)
                                    # print(rank_list)
                                    profit =(sell_price - buy_price) / buy_price
                                    final_money *= (1 + profit)
                                    # print("buy at {} at price {}, buy_epoch {}".format(buy_date, buy_price, buy_epoch))
                                    # print("Sell at {} at price {}, final money:{}, return {:.2f}%".format(sell_date, sell_price, final_money, 100 * (sell_price - buy_price) / buy_price))

                                    obj = {
                                        "buy_date":buy_date,
                                        "buy_price":buy_price,
                                        "sell_date":sell_date,
                                        "sell_price":sell_price,
                                        "final_money":round(final_money, 0),
                                        "return":round((sell_price - buy_price) / buy_price, 2)
                                    }
                                    # print("return {:.2f}%".format(100 * (sell_price - buy_price) / buy_price))
                                    detail_list.append(obj)
                                    last_sell_epoch = sell_epoch
                                    # print("last_sell_epoch => ", last_sell_epoch)
                                break
                            elif sell_epoch > sell_epoch_end:
                                # print("buy at {} at price {}, buy_epoch {}".format(buy_date, buy_price, buy_epoch))
                                print("behind sell_epoch end")

                                break

                        
                    del rank_list[0]
                    rank_list.append(trading_money_rank)
                else:
                    rank_list.append(trading_money_rank)

        period = (end_epoch - start_epoch)/ (86400 * 365)
        x = {
             "cumulative_rank_up_day": self.cumulative_rank_up_day,
             "selldout_days_after_buy": self.selldout_days_after_buy,
             "return_rate": ((final_money / init_money) - 1) / period,
             "period": period,
             "detail": detail_list
            }
        # r = final_money / init_money
        # if r > 1:
        #     print("[{} {}]: final_money {:.0f}, return +{:.2f}%".format( self.cumulative_rank_up_day, self.selldout_days_after_buy, final_money, 100 * r - 100))
        # else:
        #     print("[{} {}]: final_money {:.0f}, return -{:.2f}%".format( self.cumulative_rank_up_day, self.selldout_days_after_buy, final_money, 100 - 100 * r))
        return json.dumps(x)

    def _collect_data(self, stock_id, stock_name):
        fn = self.data_fn_fmt.format(self.stock_data_dir, stock_id, stock_name)
        stock_data = pd.read_csv(fn)
        init_money = 10000
        final_money = 10000
        detail_list = []

        element = datetime.datetime.strptime(self.collect_data_start_date,"%Y-%m-%d")
        tuple = element.timetuple()
        start_epoch = time.mktime(tuple)
        
        element = datetime.datetime.strptime(self.collect_data_end_date,"%Y-%m-%d")
        tuple = element.timetuple()
        end_epoch = time.mktime(tuple)

        rank_list = []
        buy_price = 0
        buy_date = 0
        sell_date = 0
        sell_price = 0
        last_sell_epoch = 0
        status = 0  # 1: have stock
        for index, row in stock_data.head(1).iterrows():
            trading_money = row[3]
            if trading_money < self.trading_money_min:
                # print("return None trading_money=>", trading_money)
                return None

        # print("idx from {} to {}".format(start_idx, end_idx))
        for index, row in stock_data.iterrows():
            check_date = row[0]
            close_price = row[7]
            trading_money_rank = row[10]

            if close_price == 0:
                continue

            element = datetime.datetime.strptime(check_date,"%Y-%m-%d")
            tuple = element.timetuple()
            epoch = time.mktime(tuple)
            if epoch >= start_epoch and epoch <= end_epoch:
                # print("checkdate {}".format(check_date))
                if epoch <= last_sell_epoch:
                    # print("epoch {}, last_sell_epoch {}".format(epoch, last_sell_epoch))
                    continue
                    
                if len(rank_list) > self.cumulative_rank_up_day:
                    up_order = 0
                    last_rank = rank_list[0]
                    for i in range(1, len(rank_list)):
                        if last_rank >= rank_list[i]:
                            up_order += 1
                            last_rank = rank_list[i]

                    if up_order >= self.cumulative_rank_up_day:
                        self.not_meet_up_order_criteria = 0
                        # print("check_date {}".format(check_date))
                        buy_price = row[7]
                        buy_date = check_date

                        element = datetime.datetime.strptime(buy_date,"%Y-%m-%d")
                        tuple = element.timetuple()
                        buy_epoch = time.mktime(tuple)
                        sell_epoch_start = buy_epoch + self.selldout_days_after_buy * 86400
                        sell_epoch_end = buy_epoch + (self.selldout_days_after_buy * 3) * 86400

                        # print("sell_epoch_start {}, sell_epoch_end {}".format(sell_epoch_start, sell_epoch_end))
                        # print("buy at {} at price {}, buy_epoch {}".format(buy_date, buy_price, buy_epoch))
                        for sell_index, sell_row in stock_data.iloc[index + self.selldout_days_after_buy:].iterrows():
                            # print(sell_row)
                            sell_date = sell_row[0]
                            sell_price = sell_row[7]
                            
                            element = datetime.datetime.strptime(sell_date,"%Y-%m-%d")
                            tuple = element.timetuple()
                            sell_epoch = time.mktime(tuple)
                            if sell_epoch >= sell_epoch_start and sell_epoch < sell_epoch_end:
                                if sell_price > 0:
                                    # print("criteria ", criteria)
                                    # print(rank_list)
                                    profit =(sell_price - buy_price) / buy_price
                                    final_money *= (1 + profit)
                                    # print("buy at {} at price {}, buy_epoch {}".format(buy_date, buy_price, buy_epoch))
                                    # print("Sell at {} at price {}, final money:{}, return {:.2f}%".format(sell_date, sell_price, final_money, 100 * (sell_price - buy_price) / buy_price))

                                    obj = {
                                        "buy_date":buy_date,
                                        "buy_price":buy_price,
                                        "sell_date":sell_date,
                                        "sell_price":sell_price,
                                        "final_money":round(final_money, 0),
                                        "return":round((sell_price - buy_price) / buy_price, 2)
                                    }
                                    # print("return {:.2f}%".format(100 * (sell_price - buy_price) / buy_price))
                                    detail_list.append(obj)
                                    last_sell_epoch = sell_epoch
                                    # print("last_sell_epoch => ", last_sell_epoch)
                                break
                            elif sell_epoch > sell_epoch_end:
                                # print("buy at {} at price {}, buy_epoch {}".format(buy_date, buy_price, buy_epoch))
                                print("behind sell_epoch end")

                                break

                        
                    del rank_list[0]
                    rank_list.append(trading_money_rank)
                else:
                    rank_list.append(trading_money_rank)

        period = (end_epoch - start_epoch)/ (86400 * 365)
        x = {
             "cumulative_rank_up_day": self.cumulative_rank_up_day,
             "selldout_days_after_buy": self.selldout_days_after_buy,
             "return_rate": ((final_money / init_money) - 1) / period,
             "period": period,
            #  "detail": detail_list
            }
        # r = final_money / init_money
        # if r > 1:
        #     print("[{} {}]: final_money {:.0f}, return +{:.2f}%".format( self.cumulative_rank_up_day, self.selldout_days_after_buy, final_money, 100 * r - 100))
        # else:
        #     print("[{} {}]: final_money {:.0f}, return -{:.2f}%".format( self.cumulative_rank_up_day, self.selldout_days_after_buy, final_money, 100 - 100 * r))
        return json.dumps(x)
        

