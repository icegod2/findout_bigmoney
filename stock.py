import optparse
import os
import shutil
import math
import pandas as pd
import requests
from FinMind.data import DataLoader
from FinMind import plotting
from random import randint
from time import sleep
from datetime import date
from strategy_trading_money_rank  import Trading_money_rank
import json




stocklist_fn = "stocklist.csv"
save_data_folder = "./db/"
data_fn_fmt = "{}{}_{}"



def get_trading_money_rank_each_day(check_date):
    dict = {}
    print("check date => {}".format(check_date))
    stocklist = pd.read_csv(stocklist_fn)
    for index, row in stocklist.iterrows():
        stock_id = int(row['有價證券代號'])
        stock_name = row['有價證券名稱']
        fn = data_fn_fmt.format(save_data_folder, stock_id, stock_name)
        try:
            stockdata = pd.read_csv(fn)
            # print("doing fn {}".format(fn))
            new_df = stockdata[stockdata['date'].str.contains(check_date, na=False)]
            if not new_df.empty:
                dict[fn] = stockdata['Trading_money'][stockdata.date==check_date].values[0]
        except:
            print("An exception occurred fn:{}".format(fn))

    dict = sorted(dict.items(), key=lambda item: item[1], reverse=True)
    # print(dict) 
    cnt = 1
    for stock_fn, money in dict:
        # print(stock_fn, money)
        stockdata = pd.read_csv(stock_fn)
        stock_fn_tmp = stock_fn + ".tmp"
        new_df = stockdata[stockdata['date'].str.contains(check_date, na=False)]
        if not new_df.empty:
            row_num = stockdata[stockdata['date'] == check_date].index[0]
            stockdata.at[row_num, 'Trading_money_rank'] = cnt
            # print(stockdata)
            stockdata.to_csv(stock_fn_tmp, sep=',', index=False, encoding='utf-8')
            shutil.copy(stock_fn_tmp, stock_fn)
            os.remove(stock_fn_tmp)
            cnt += 1




def update_trading_money_rank():
    stocklist = pd.read_csv(stocklist_fn)
    stock_id, stock_name = stocklist.iloc[0]
    # print(stock_id, stock_name)
    fn = data_fn_fmt.format(save_data_folder, stock_id, stock_name)
    stock_data = pd.read_csv(fn)
    for index, row in stock_data.iterrows():
        check_date = row[0]
        trading_money_rank = row[10]
        if trading_money_rank != -1:
            continue
        break

    start_idx = index - 2
    for index, row in stock_data.iloc[start_idx:].iterrows():
        check_date = row[0]
        get_trading_money_rank_each_day(check_date)


def update_stock_list():
    res = requests.get(
        "https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=1&industry_code=&Page=1&chklike=Y")
    # Parse the Source Code into a Pandas DataFrame
    df = pd.read_html(res.text)[0]
    df = df.drop([0, 1, 4, 5, 6, 7, 8, 9], axis=1)  # Drop Useless Columns
    df.columns = df.iloc[0]               # Replace DataFrame Columns Title
    df = df.iloc[1:]
    df.to_csv(stocklist_fn, sep=',', index=False, encoding='utf-8')
    return df


def update_stock_data(start=1):
    stocklist = pd.read_csv(stocklist_fn)
    for index, row in stocklist.iterrows():
        curr_stock_id = int(row['有價證券代號'])
        curr_stock_name = row['有價證券名稱']
        if curr_stock_id >= start:
            get_stock_info(curr_stock_id, curr_stock_name)
            sleep(randint(2, 5))

def get_last_udpate_day(stock_id, stock_name):
    fn = data_fn_fmt.format(save_data_folder, stock_id, stock_name)
    print(fn)
    stock_data = pd.read_csv(fn)
    for x in range(-1, -5, -1):
        d = stock_data.values[x][0]
        if type(d) == str:
            return x, d
    return None

def get_stock_info(stock_id, stock_name):
    if not os.path.exists(save_data_folder):
        os.mkdir(save_data_folder)
        print("Directory '% s' created" % save_data_folder)

    dl = DataLoader()
    last_row, last_day = get_last_udpate_day(stock_id, stock_name)
    if last_day is None:
        print("last day is none stock_name:{}".format(stock_name))
        return

    today = date.today()
    new_data = dl.taiwan_stock_daily(
        stock_id=stock_id, start_date=last_day, end_date=today)
    new_data['Trading_money_rank'] = -1
    row, col = new_data.shape
    if row > 3:
        fn = data_fn_fmt.format(save_data_folder, stock_id, stock_name)
        fn_tmp = fn + ".tmp"
        stock_data = pd.read_csv(fn)
        m = pd.concat([stock_data.iloc[:last_row],new_data],ignore_index=1)
        m.to_csv(fn_tmp, sep=',', index=False, encoding='utf-8')
        shutil.copy(fn_tmp, fn)
        os.remove(fn_tmp)


def show_stock_info(stock_id, stock_name):
    fn = data_fn_fmt.format(save_data_folder, stock_id, stock_name)
    plot_fn = "{}.html".format(fn)
    stockdata = pd.read_csv(fn)
    print(stockdata)
    plotting.kline(stockdata, filename=plot_fn)



def do_trading_money_rank():
    s = Trading_money_rank()
    print(s.start_date)
    print(s.end_date)
    stocklist = pd.read_csv(stocklist_fn)
    profit_dic =dict()
    last_upper_day=0
    last_down_day=0
    for index, row in stocklist.head(1).iterrows():
        curr_stock_id = int(row['有價證券代號'])
        curr_stock_name = row['有價證券名稱']
        print(curr_stock_id, curr_stock_name)

        for day_interval in range(5, 6):
            for upper_day in range(0, day_interval, 2):
                for down_day in range(day_interval, -1, -2):
                    if upper_day == last_upper_day and down_day == last_down_day:
                        continue
                    s.set_day_interval(day_interval)
                    s.set_upper_day(upper_day)
                    s.set_down_day(down_day)
                    print(day_interval, upper_day, down_day)
                    j_ret= s.start(curr_stock_id, curr_stock_name) 
                    print(j_ret)
                    # content="[{} {} {}] traction:{}, profile:{:.2f}% ".format(s.get_day_interval(), s.get_upper_day(), s.get_down_day(), traction, 100 * profit)
                    # print(content)
                    # str_profit = str(round(profit, 0))
                    # print(str_profit)
                    # if str_profit in profit_dic:
                    #     # tmp = []
                    #     print("get duplicate =〉", str_profit)
                    #     # tmp.append(str)
                    #     profit_dic[str_profit].append([content, detail])
                    # else:
                    #     profit_dic[str_profit] = [content, detail]
                    last_upper_day=upper_day
                    last_down_day=down_day

        # print(max(profit_dic))
        # print(min(profit_dic))
        # profit_dic2 = dict(sorted(profit_dic.items(), reverse=True))

        # profit_dic.sort(reverse = True)
        # print(profit_dic)
        # cnt = 0
        # for key in profit_dic2:
        #     if cnt > 5:
        #         break
        #     for e in profit_dic2[key]:
        #         print(e)
            # l = profit_dic2[key]
            # print(key, "=>", l[0])
            # while not l[1].empty():
            #     s = l[1].get()
            #     print(s)




        # print("get profile {:.2f}".format(profite))





def main():
    parser = optparse.OptionParser(usage="%prog [-u] [-t]", version="%prog 1.0")
    parser.add_option('-u', dest='type',
                      type='string',
                      help='update [list| data]')

    parser.add_option('-s', dest='num',
                      type='int',
                      help='update [list| data] start from NUM')


    parser.add_option('-S', dest='strategy',
                      type='str',
                      help='which strategy want to use')
    
    (options, args) = parser.parse_args()

    if options.type != None:
        if options.type == "list":
            update_stock_list()
        elif options.type == "data":
            if options.num != None:
                update_stock_data(options.num)
            else:
                update_stock_data()
            update_trading_money_rank()
        else:
            print(parser.usage)
            exit(1)


    if options.strategy != None:
        if options.strategy == "trading_money_rank":
            do_trading_money_rank()


if __name__ == "__main__":
    main()
