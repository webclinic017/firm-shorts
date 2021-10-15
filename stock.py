import json
from os import path
import requests
import pandas
import datetime as dt
import time


# |---------------------------------|
# | THIS DOES NOT WORK BC POLYGON   |
# | UPDATED THEIR PLAN - THEY GOT   |
# | CAUGHT STEALING DATA FROM ALPACA|
# | SO ... I AM NOT AUTHORIZED      |
# | NEED TO SWITCH TO yfinance      |
# |---------------------------------|

# CONSTANTS
KEY = 'xxxx'  # polygon.io API key
DATE = dt.datetime.today().strftime('%Y-%m-%d')
BIOTECH_STOCKS = ['TTNP', 'DXCM', 'NVAX', 'PDEX', 'VRTX', 'NBIX', 'INFU', 'CODX', 'GDRX', 'FLGT', 'PODD', 'NVCR']
SPACS = ['GHIV', 'THCB', 'STPK', "PCPL", 'IPOD', 'IPOE', 'GOEV', 'SFVA', "CCIV", "OCA", "OMEG", "FUSE"]
MEME_STOCKS = ['GME', 'AMC', 'NOK', 'BB', 'TSLA']

# VARS
t_req = 0  # total requests
today = dt.datetime.today().strftime('%Y-%m-%d')
two_years_ago = (dt.datetime.today() - dt.timedelta(365 * 2)).strftime('%Y-%m-%d')


# call this function every time a request is made to avoid exceeding the rate limit
def rate_limit():
    global t_req
    t_req += 1
    if t_req % 5 == 0:
        print('RATE LIMIT MET OR EXCEEDED | SLEEPING FOR SIXTY SECONDS')
        time.sleep(60)


class Stock:

    def __init__(self, ticker: str):
        self.ticker = ticker

        # TRY TO OPEN WHAT WE HAVE, ELSE DOWNLOAD IT
        try:
            dat = self.agg_data(False)  # locally, regardless of timeframe
        except FileNotFoundError:
            dat = self.agg_data(True)  # download from two years ago today

        # INSTANCE ATTRIBUTES
        self.adjusted = dat['adjusted']  # boolean
        self.count = dat['resultsCount']

        # CONSTRUCT A PANDAS TIME-SERIES
        df = pandas.DataFrame(dat['results'])
        df['t'] = pandas.to_datetime(df['t'], unit='ms')  # convert from UNIX timestamp
        self.timeline = df.set_index(df['t'].dt.date)
        self.timeline['tas'] = self.timeline.index.astype(str)  # TAS time as string

    # get aggregate data (bars) for a stock up to two years ago
    def agg_data(self, download: bool, multiplier=1, timespan='day', start=two_years_ago, end=today,
                 unadjusted='false', limit=5000):

        # REQUEST FROM POLYGON API
        if download:
            resp = requests.get(
                f'https://api.polygon.io/v2/aggs/ticker/{self.ticker}/range/{multiplier}/{timespan}/{start}/{end}?'
                f'unadjusted={unadjusted}&sort=asc&limit={limit}&apiKey={KEY}').json()
            print('MADE REQUEST')

            # SAVE THAT Stuff
            with open(f'./data/dynamic/stock/price/{self.ticker}.json', 'w') as f:
                print('SAVED ' + self.ticker)  # DEBUG
                json.dump(resp, f, indent=4)
            rate_limit()

        # OR JUST OPEN IF DESIRED (WILL THROW ERROR IF DNE)
        else:
            with open(f'./data/dynamic/stock/price/{self.ticker}.json') as f:
                resp = json.load(f)

        return resp

    @staticmethod
    def get_snapshot(download: bool, tickers=None):
        with open('./data/dynamic/stock/market/today.json', 'w+') as f:
            if download:
                url = 'https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apikey=' + KEY
                resp = requests.get(url).json()
                json.dump(resp, f)
                return resp
            else:
                return json.load(f)

    # checks to see if there is saved data about the ticker, returns boolean
    def has_existing_data(self):
        return path.exists(f'./data/dynamic/stock/price/{self.ticker}.json')

    # returns price change over two dates
    def return_over(self, start: str, end: str):
        return float((self.timeline.at[end, 'c'] - self.timeline.at[start, 'o']) / self.timeline.at[start, 'o'])

    # returns a merged dataframe of Reddit subscriber data and stock price data
    def compare_to_sub(self, subreddit='wallstreetbets'):  # other options are stocks, options, robinhood
        """:returns merged pandas Dataframe of stock price (H, L, O, C, V) and subreddit subscriber data"""
        # OPEN THE SUBSCRIBER DATA AND LOAD IT IN AS PD DATAFRAME
        with open(f'./data/static/subscriber/{subreddit}.csv') as f:
            sub_df = pandas.read_csv(f, sep=';', parse_dates=True)
        sub_df.set_index('date', inplace=True)
        sub_df['tas'] = sub_df.index.astype(str)  # TAS time as string

        # MERGE AND RETURN THE DATAFRAME OF PRICE AND SUBSCRIBER DATA
        joint_df = self.timeline.merge(sub_df, how='left', on='tas')  # TAS time as string
        joint_df.set_index('tas', inplace=True)
        return joint_df


if __name__ == "__main__":
    amc = Stock('AMC')
    amc.get_snapshot(True)
