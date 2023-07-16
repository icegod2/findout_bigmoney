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
import datetime as dt
import time

from notifyUser import NotifyUser



stocklist_fn = "stocklist.csv"
log_fn = "report.log"
stock_data_dir = "./db/"
data_fn_fmt = "{}{}_{}"



def get_trading_money_rank_each_day(check_date):
    dict = {}
    print("check date => {}".format(check_date))
    stocklist = pd.read_csv(stocklist_fn)
    for index, row in stocklist.iterrows():
        stock_id = int(row['有價證券代號'])
        stock_name = row['有價證券名稱']
        fn = data_fn_fmt.format(stock_data_dir, stock_id, stock_name)
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
    fn = data_fn_fmt.format(stock_data_dir, stock_id, stock_name)
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
            sleep(randint(10, 20))

def update_one_stock_data(stock_id, start_date_from):
    stocklist = pd.read_csv(stocklist_fn)
    stock = stocklist[(stocklist['有價證券代號'] == stock_id)].head(1)
    for index, row in stock.iterrows():
        curr_stock_id = int(row['有價證券代號'])
        curr_stock_name = row['有價證券名稱']
        if curr_stock_id == stock_id:
            get_one_stock_info(curr_stock_id, curr_stock_name, start_date_from)


def get_last_udpate_day(stock_id, stock_name):
    fn = data_fn_fmt.format(stock_data_dir, stock_id, stock_name)
    print(fn)
    stock_data = pd.read_csv(fn)
    for x in range(-1, -5, -1):
        d = stock_data.values[x][0]
        if type(d) == str:
            return x, d
    return None


def get_one_stock_info(stock_id, stock_name, start_date):
    if not os.path.exists(stock_data_dir):
        os.mkdir(stock_data_dir)
        print("Directory '% s' created" % stock_data_dir)

    dl = DataLoader()
    today = date.today()
    new_data = dl.taiwan_stock_daily(
        stock_id=stock_id, start_date=start_date, end_date=today)
    new_data['Trading_money_rank'] = -1
    row, col = new_data.shape

    start_epoch = dt.datetime.strptime(start_date,"%Y-%m-%d").timestamp()
    if row > 3:
        fn = data_fn_fmt.format(stock_data_dir, stock_id, stock_name)
        fn_tmp = fn + ".tmp"
        stock_data = pd.read_csv(fn)
        for index, row in stock_data.iterrows():
            check_date = row[0]
            row_epoch = dt.datetime.strptime(check_date,"%Y-%m-%d").timestamp()
            if (start_epoch <= row_epoch):
                break
        print("index => ", index)
        print("row => ", row)

        m = pd.concat([stock_data.iloc[:index],new_data],ignore_index=1)
        m.to_csv(fn_tmp, sep=',', index=False, encoding='utf-8')
        shutil.copy(fn_tmp, fn)
        os.remove(fn_tmp)

    print(stock_id, stock_name, start_date)




def get_stock_info(stock_id, stock_name):
    if not os.path.exists(stock_data_dir):
        os.mkdir(stock_data_dir)
        print("Directory '% s' created" % stock_data_dir)

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
        fn = data_fn_fmt.format(stock_data_dir, stock_id, stock_name)
        fn_tmp = fn + ".tmp"
        stock_data = pd.read_csv(fn)
        m = pd.concat([stock_data.iloc[:last_row],new_data],ignore_index=1)
        m.to_csv(fn_tmp, sep=',', index=False, encoding='utf-8')
        shutil.copy(fn_tmp, fn)
        os.remove(fn_tmp)


def show_stock_info(stock_id, stock_name):
    fn = data_fn_fmt.format(stock_data_dir, stock_id, stock_name)
    plot_fn = "{}.html".format(fn)
    stockdata = pd.read_csv(fn)
    print(stockdata)
    plotting.kline(stockdata, filename=plot_fn)


def notify_user(buy_list):
    notify = NotifyUser("smtp.gmail.com", 587, "XXXXXX", "test@gmail.com")
    notify.add_notify_user_email("test@gmail.com")

    html = """\
    <html>
    <body>
        <p>Hi,<br>
        Today you can buy<br>
    """

    for stock in buy_list:
        html += '<a href="https://tw.stock.yahoo.com/quote/{}.TW">{} {}</a><br>'.format(stock[0], stock[0], stock[1])
    
    html += """</p>
    </body>
    </html>
    """

    notify.send_email(html)


def main():
    parser = optparse.OptionParser(usage="%prog [-u] [-t]", version="%prog 1.0")
    parser.add_option('--update', dest='type',
                      type='string',
                      help='update [list| data | buy_rule]')

    parser.add_option('--start_id', dest='start_stock_id',
                      type='int',
                      help='update data start from stock_id')

    parser.add_option('--target_id', dest='target_stock_id',
                      type='int',
                      help='update data target stock_id')

    parser.add_option('--start_date', dest='from_target_date',
                      type='string',
                      help='update data target stock_id from date (e.g. 2022-07-21)')

    parser.add_option('-S', dest='strategy',
                      type='str',
                      help='which strategy want to use')
    
    (options, args) = parser.parse_args()
    print(options)
    if options.type != None:
        if options.type == "list":
            update_stock_list()
        elif options.type == "buy_rule":
            s = Trading_money_rank()
            s.update_data_start()
            s.update_buy_criteria()
        elif options.type == "data":
            if options.start_stock_id != None: 
                update_stock_data(options.start_stock_id)
            elif options.target_stock_id != None: 
                update_one_stock_data(options.target_stock_id, options.from_target_date)
            else:
                update_stock_data()
            update_trading_money_rank()
        else:
            print(parser.usage)
            exit(1)


    if options.strategy != None:
        if options.strategy == "trading_money_rank":
            update_stock_data()
            update_trading_money_rank()
            s = Trading_money_rank()
            buy_list = s.pick_up_stock()
            notify_user(buy_list)



if __name__ == "__main__":
    main()
