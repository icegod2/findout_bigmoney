import pandas as pd
import time
import datetime
import queue
import numpy as np
import json

class Trading_money_rank:
    # 建構式rank_up_day_cnt
    def __init__(self):
        self.day_interval = 20 
        self.rank_up_day_cnt = 13 
        self.rank_down_day_cnt = 10 
        self.trading_money_min = 500000000
        self.training_start_date = "2015-1-01"
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
    
    def set_day_interval(self, val):
        self.day_interval = val
    
    def set_rank_up_day_cnt(self, val):
        self.rank_up_day_cnt = val
    
    def set_rank_down_day_cnt(self, val):
        self.rank_down_day_cnt = val
    
    def get_training_start_date(self):
        return self.training_start_date
    
    def get_training_end_date(self):
        return self.training_end_date
    
    def get_day_interval(self):
        return self.day_interval

    def get_rank_up_day_cnt(self):
        return self.rank_up_day_cnt
    
    def get_rank_down_day_cnt(self):
        return self.rank_down_day_cnt
    
    def train(self, stock_id, stock_name):
        # print(stock_id)
        # print(stock_name)
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
                if len(rank_list) > self.day_interval:
                    del rank_list[0]
                    rank_list.append(trading_money_rank)
                    up_order = 0
                    down_order = 0
                    criteria=rank_list[0]
                    for i in range(len(rank_list)):
                        if rank_list[i] < criteria:
                            up_order += 1
                        else:
                            down_order +=1
                    if status == 0 and up_order >= self.rank_up_day_cnt:
                        buy_price = row[7]
                        buy_date = check_date
                        status = 1
                        # print("buy at {} at price {}".format(buy_date, buy_price))
                    elif status == 1 and down_order >= self.rank_down_day_cnt:
                        sell_date = check_date
                        sell_price = row[7]
                        # print("Sell at {} at price {}".format(sell_date, buy_price))
                        profit =(sell_price - buy_price) / buy_price
                        final_money *= (1 + profit)

                        element = datetime.datetime.strptime(buy_date,"%Y-%m-%d")
                        tuple = element.timetuple()
                        buy_date_epoch = time.mktime(tuple)
                        
                        element = datetime.datetime.strptime(sell_date,"%Y-%m-%d")
                        tuple = element.timetuple()
                        sell_date_epoch = time.mktime(tuple)

                        holding_day = (sell_date_epoch - buy_date_epoch) / 86400


                        obj = {
                            "buy_date":buy_date,
                            "buy_price":buy_price,
                            "sell_date":sell_date,
                            "sell_price":sell_price,
                            "final_money":final_money,
                            "holding_days":holding_day,
                            "return":(sell_price - buy_price) / buy_price
                        }
                        detail_list.append(obj)
                        status = 0
                else:
                    rank_list.append(trading_money_rank)

        x = {
             "day_interval": self.day_interval,
             "rank_up_day_cnt": self.rank_up_day_cnt,
             "rank_down_day_cnt": self.rank_down_day_cnt,
             "traction_num": len(detail_list),
             "return_rate": final_money / init_money,
             "detail": detail_list
            }
        return json.dumps(x)
        

