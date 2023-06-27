import pandas as pd
import time
import datetime
import queue
import numpy as np
import json

class Trading_money_rank:
    # 建構式
    def __init__(self):
        self.day_interval = 20 
        self.upper_day = 13 
        self.down_day = 10 
        self.start_date = "2001-1-01"
        self.end_date = "2022-12-31"
        self.data_fn_fmt = "{}{}_{}"
        self.save_data_folder = "./db/"
    def set_start_date(self, day):
        self.start_date = day

    def set_data_fn_fmt(self, fmt):
        self.data_fn_fmt = fmt

    def set_save_data_folder(self, path):
        self.save_data_folder = path

    def set_end_date(self, day):
        self.end_date = day
    
    def set_day_interval(self, val):
        self.day_interval = val
    
    def set_upper_day(self, val):
        self.upper_day = val
    
    def set_down_day(self, val):
        self.down_day = val
    
    def get_start_date(self):
        return self.start_date
    
    def get_end_date(self):
        return self.end_date
    
    def get_day_interval(self):
        return self.day_interval

    def get_upper_day(self):
        return self.upper_day
    
    def get_down_day(self):
        return self.down_day
    
    def start(self, stock_id, stock_name):
        # print(stock_id)
        # print(stock_name)
        fn = self.data_fn_fmt.format(self.save_data_folder, stock_id, stock_name)
        stock_data = pd.read_csv(fn)
        init_money = 10000
        start_money = 10000

        start_epoch=0
        end_epoch=0
        q = queue.Queue()

        element = datetime.datetime.strptime(self.start_date,"%Y-%m-%d")
        tuple = element.timetuple()
        start_epoch = time.mktime(tuple)
        
        element = datetime.datetime.strptime(self.end_date,"%Y-%m-%d")
        tuple = element.timetuple()
        end_epoch = time.mktime(tuple)

        rank_list = []
        buy_price = 0
        buy_date = 0
        sell_date = 0
        sell_price = 0
        status = 0  # 1: have stock
        # print("idx from {} to {}".format(start_idx, end_idx))
        for index, row in stock_data.iterrows():
            check_date = row[0]
            trading_money_rank = row[10]
            close_price = row[7]
            if close_price == 0:
                continue

            element = datetime.datetime.strptime(check_date,"%Y-%m-%d")
            tuple = element.timetuple()
            epoch = time.mktime(tuple)
            if epoch >= start_epoch and epoch <= end_epoch:
                if len(rank_list) >= self.day_interval:
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
                    if status == 0 and up_order >= self.upper_day:
                        buy_price = row[7]
                        buy_date = check_date
                        status = 1
                        # print("buy at {} at price {}".format(buy_date, buy_price))
                    elif status == 1 and down_order >= self.down_day:
                        sell_date = check_date
                        sell_price = row[7]
                        # print("Sell at {} at price {}".format(sell_date, buy_price))
                        profit =(sell_price - buy_price) / buy_price
                        start_money *= (1 + profit)

                        element = datetime.datetime.strptime(buy_date,"%Y-%m-%d")
                        tuple = element.timetuple()
                        buy_date_epoch = time.mktime(tuple)
                        
                        element = datetime.datetime.strptime(sell_date,"%Y-%m-%d")
                        tuple = element.timetuple()
                        sell_date_epoch = time.mktime(tuple)

                        holding_day = (sell_date_epoch - buy_date_epoch) / 86400


                        str="{} buy at price:{}, {} sell at price {}, start_money:{:.2f}, profile:{:.2f}% ( {} days )\n".format(buy_date, buy_price, sell_date, sell_price, start_money, 100 * profit, holding_day)
                        q.put(str)
                        status = 0
                else:
                    rank_list.append(trading_money_rank)


        traction = q.qsize()
        s = ""
        while not q.empty():
            s += q.get()
            s += ","
        x = {
             "day_interval": str(self.day_interval),
             "upper_day": str(self.upper_day),
             "down_day": str(self.down_day),
             "profit": str(start_money / init_money),
             "traction_num": str(traction),
             "detail_q": s
            }
        return json.loads(x)
        

