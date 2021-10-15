import requests
import datetime as dt
import json
import time
import matplotlib.pyplot as plt
import pandas as pd
from stock import Stock

# CONSTANTS
KEY = 'xxxx'  # ADD KEY
APD = 16847.611650485436  # average WSB per day since dec 01, 2019

# VARS
today = dt.datetime.today().strftime('%Y-%m-%d')
two_years_ago = (dt.datetime.today() - dt.timedelta(365 * 2)).strftime('%Y-%m-%d')


def graph_subs_volume_price(stonk: Stock, sub='wallstreetbets', vol_scale=1000, price_scale=100000):
    df = stonk.compare_to_sub(sub)
    print(df)

    # VARS
    volumes = df['v']/vol_scale
    # wsb = df['subs']
    wsb_per_day = df['subs'].diff()
    # prices = df['c']*price_scale

    # PLOT
    # plt.plot(wsb, color='b')
    plt.plot(wsb_per_day, color='y')
    plt.plot(volumes.index, volumes)
    # plt.plot(prices, color='g')
    plt.show()


def graph_accounts():
    with open(r'./data/dynamic/trend/hot_accounts.json') as f:
        account_df = pd.read_json(f)
    return account_df


# THEN play with the data
if __name__ == "__main__":
    df = graph_accounts()
    df.sort_values(by='comments', ascending=False, inplace=True)
    print(df[1:11])  # omitting index 0 bc the user in None
    df[1:11].plot.bar()
    plt.show()
