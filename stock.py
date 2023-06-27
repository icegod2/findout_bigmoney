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
import itertools



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



def do_trading_money_rank_strategy():
    s = Trading_money_rank()
    print(s.training_start_date)
    print(s.training_end_date)
    stocklist = pd.read_csv(stocklist_fn)
    strategy_dict = dict()
    last_rank_up_day_cnt=0
    last_rank_down_day_cnt=0
    for index, row in stocklist.head(30).iterrows():
        curr_stock_id = int(row['有價證券代號'])
        curr_stock_name = row['有價證券名稱']
        do_stop = 0
        return_rate_dic = dict()
        print(curr_stock_id, curr_stock_name)

        for day_interval in range(5, 10):
            if do_stop == 1:
                break;
            for rank_up_day_cnt in range(0, day_interval, 1):
                if do_stop == 1:
                    break;
                for rank_down_day_cnt in range(day_interval, -1, -1):
                    if rank_up_day_cnt == last_rank_up_day_cnt and rank_down_day_cnt == last_rank_down_day_cnt:
                        continue
                    s.set_day_interval(day_interval)
                    s.set_rank_up_day_cnt(rank_up_day_cnt)
                    s.set_rank_down_day_cnt(rank_down_day_cnt)
                    j_ret= s.train(curr_stock_id, curr_stock_name) 
                    if j_ret == None:
                        do_stop = 1
                        continue;
                    j_ret=json.loads(j_ret)

                    s_return_rate = str(round(j_ret['return_rate'], 2))
                    if s_return_rate in return_rate_dic:
                        # tmp = []
                        # print("get duplicate =〉", s_return_rate)
                        # tmp.append(str)
                        return_rate_dic[s_return_rate].append(j_ret)
                    else:
                        return_rate_dic[s_return_rate] = [j_ret]
                    last_rank_up_day_cnt=rank_up_day_cnt
                    last_rank_down_day_cnt=rank_down_day_cnt


        if do_stop == 0:
            print("max return: {}%".format(100 * float(max(return_rate_dic))))
            print("min return: {}%".format(100 * float(min(return_rate_dic))))
            # print("min return: {}%".format(100 * min(return_rate_dic)))
            return_rate_dic = dict(sorted(return_rate_dic.items(), reverse=True))
            return_rate_dic = dict(itertools.islice(return_rate_dic.items(), 1,6))


            for key in return_rate_dic:
                for j_ret in return_rate_dic[key]:
                    s_key = "{}_{}_{}".format(j_ret['day_interval'], j_ret['rank_up_day_cnt'], j_ret['rank_down_day_cnt'])
                    if strategy_dict.get(s_key):
                        strategy_dict[s_key] += 1
                    else:
                        strategy_dict[s_key] = 1
                    print("[{} {} {}] tranctions:{}, return_rate:{:.2f}%".format(j_ret['day_interval'], j_ret['rank_up_day_cnt'], j_ret['rank_down_day_cnt'], j_ret['traction_num'], 100 * j_ret['return_rate']))

    print(strategy_dict)





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
            do_trading_money_rank_strategy()


if __name__ == "__main__":
    main()
