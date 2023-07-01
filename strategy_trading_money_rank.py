import pandas as pd
import time
import datetime
import queue
import numpy as np
import json

class Trading_money_rank:
    # 建構式rank_up_day_cnt
    def __init__(self):
        self.cumulative_rank_up_day = 5 
        self.selldout_days_after_buy = 10
        self.trading_money_min = 500000000
        self.training_start_date = "2020-01-01"
        self.training_end_date = "2022-12-31"
        self.valid_start_date = "2023-1-01"
        self.valid_end_date = "2023-12-31"
        self.data_fn_fmt = "{}{}_{}"
        self.save_data_folder = "./db/"
    def set_training_start_date(self, day):
        self.training_start_date = day

    def set_data_fn_fmt(self, fmt):
        self.data_fn_fmt = fmt

    def set_save_data_folder(self, path):
        self.save_data_folder = path

    def set_training_end_date(self, day):
        self.training_end_date = day
    
    def set_cumulative_rank_up_day(self, val):
        self.cumulative_rank_up_day = val

    def set_selldout_days_after_buy(self, val):
        self.selldout_days_after_buy = val
    
    def get_training_start_date(self):
        return self.training_start_date
    
    def get_training_end_date(self):
        return self.training_end_date
    
    def get_cumulative_rank_up_day(self):
        return self.cumulative_rank_up_day

    def get_selldout_days_after_buy(self):
        return self.selldout_days_after_buy

    def train(self, stock_id, stock_name):
        fn = self.data_fn_fmt.format(self.save_data_folder, stock_id, stock_name)
        stock_data = pd.read_csv(fn)
        init_money = 10000
        final_money = 10000

        start_epoch=0
        end_epoch=0
        detail_list = []
        element = datetime.datetime.strptime(self.training_start_date,"%Y-%m-%d")
        tuple = element.timetuple()
        start_epoch = time.mktime(tuple)
        
        element = datetime.datetime.strptime(self.training_end_date,"%Y-%m-%d")
        tuple = element.timetuple()
        end_epoch = time.mktime(tuple)

        rank_list = []
        buy_price = 0
        buy_date = 0
        sell_date = 0
        sell_price = 0
        status = 0  # 1: have stock

        # for index, row in stock_data.head(1).iterrows():
        #     trading_money = row[3]
        #     if trading_money < self.trading_money_min:
        #         print("return None trading_money=>", trading_money)
        #         return None

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
                                    # print("Sell at {} at price {}, final money:{}, return {:.2f}%".format(sell_date, sell_price, final_money, 100 * (sell_price - buy_price) / buy_price))

                                    obj = {
                                        "buy_date":buy_date,
                                        "buy_price":buy_price,
                                        "sell_date":sell_date,
                                        "sell_price":sell_price,
                                        "final_money":final_money,
                                        "return":(sell_price - buy_price) / buy_price
                                    }
                                    # print("return {:.2f}%".format(100 * (sell_price - buy_price) / buy_price))
                                    detail_list.append(obj)
                                break
                            elif sell_epoch > sell_epoch_end:
                                print("buy at {} at price {}, buy_epoch {}".format(buy_date, buy_price, buy_epoch))
                                print("behind sell_epoch end")

                                break

                        
                    del rank_list[0]
                    rank_list.append(trading_money_rank)
                else:
                    rank_list.append(trading_money_rank)

        x = {
             "cumulative_rank_up_day": self.cumulative_rank_up_day,
             "selldout_days_after_buy": self.selldout_days_after_buy,
             "return_rate": final_money / init_money
            #  "detail": detail_list
            }
        # r = final_money / init_money
        # if r > 1:
        #     print("[{} {}]: final_money {:.0f}, return +{:.2f}%".format( self.cumulative_rank_up_day, self.selldout_days_after_buy, final_money, 100 * r - 100))
        # else:
        #     print("[{} {}]: final_money {:.0f}, return -{:.2f}%".format( self.cumulative_rank_up_day, self.selldout_days_after_buy, final_money, 100 - 100 * r))
        return json.dumps(x)
        

