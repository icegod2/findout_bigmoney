import optparse
import os
import pandas as pd
import requests
from FinMind.data import DataLoader
from FinMind import plotting
from random import randint
from time import sleep


stocklist_fn = "stocklist.csv"
save_data_folder="./db/"
data_fn_fmt="{}{}_{}"

def update_stock_list():
  res = requests.get("https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=1&industry_code=&Page=1&chklike=Y")
  df = pd.read_html(res.text)[0]        # Parse the Source Code into a Pandas DataFrame
  df = df.drop([0,1,4,5,6,7,8,9],axis = 1)  # Drop Useless Columns
  df.columns = df.iloc[0]               # Replace DataFrame Columns Title
  df = df.iloc[1:]
  df.to_csv(stocklist_fn, sep=',', index=False, encoding='utf-8')
  return df

def update_stock_data():
    stocklist = pd.read_csv(stocklist_fn)
    for index, row in stocklist..iterrows():
        curr_stock_id = int(row['有價證券代號'])
        curr_stock_name = row['有價證券名稱']
        get_stock_info(curr_stock_id, curr_stock_name)
        sleep(randint(10,30))

def get_stock_info(stock_no, stock_name):
    if not os.path.exists(save_data_folder):
        os.mkdir(save_data_folder)
        print("Directory '% s' created" % save_data_folder)

    dl = DataLoader()
    stock_data = dl.taiwan_stock_daily(stock_id=stock_no, start_date='2000-01-01', end_date='2022-12-31')
    plotting.kline(stock_data)
    fn=data_fn_fmt.format(save_data_folder, stock_no, stock_name)
    print("save to {}".format(fn))
    if os.path.exists(fn):
        os.remove(fn)

    stock_data.to_csv(fn, sep=',', index=False, encoding='utf-8')


def show_stock_info(stock_no, stock_name):
    print("show_stock_info get {} {}".format(stock_no, stock_name))
    fn=data_fn_fmt.format(save_data_folder, stock_no, stock_name)
    plot_fn = "{}.html".format(fn)
    print("fn => {}".format(fn))
    print("plot_fn => {}".format(plot_fn))
    stocklist = pd.read_csv(fn)
    plotting.kline(stocklist, filename=plot_fn)



def test_callback(stock_no):
    stocklist = pd.read_csv(stocklist_fn)
    print("test_callback get stock_no:{}".format(stock_no))
    for index, row in stocklist.head(10).iterrows():
        curr_stock_id = int(row['有價證券代號'])
        curr_stock_name = row['有價證券名稱']
        if curr_stock_id == stock_no:
            show_stock_info(curr_stock_id, curr_stock_name)
            break



def main():
    parser = optparse.OptionParser(usage="%prog [-u]", version="%prog 1.0")
    parser.add_option('-u', dest = 'type',
                      type = 'string',
                      help = 'update [ list| data]')

    parser.add_option('-t', dest = 'id',
                      type = 'int',
                      help = 'test stock_id')

    (options, args) = parser.parse_args()
    if options.type != None:
        if options.type == "list":
            update_stock_list()
        elif options.type == "data":
            update_stock_data()
        else:
            print(parser.usage)
            exit(0)

    print(options.id)
    if options.id != None:
        test_callback(options.id)

if __name__ == "__main__":
    main()
