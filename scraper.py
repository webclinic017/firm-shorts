from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import os
import pandas as pd
import time


# Change non-numeric values to numeric & change column names to a more readable format
def trend_cleaner(filepath):

    # just open & load it
    trends = pd.read_csv(filepath, header=1)

    # the columns are all strings and it will either be an number 0-100 or '<1', which is a problem. I convert it to int
    # or I just replace it with 0.5. The columns also have a region suffix like SEARCH_TERM: United States so I remove
    # the trailing part and make it all uppercase because I'll almost exclusively be checking stock tickers
    for col in trends:
        if col == 'Week':
            pass
        else:
            trends[col] = trends[col].apply(lambda x: 0.5 if x == '<1' else int(x))  # str to int or double...
            new_col_name = col.split(':')[0].upper()
            trends.rename(columns={col: new_col_name}, inplace=True)
    trends.to_csv("./data/dynamic/cache/multiTimeline.csv")  # save it in dynamic data


class Scraper:
    TRENDS_FP = "./data/dynamic/trend/multiTimeline.csv"

    def __init__(self):
        self.endpoints = []
        self.driver = webdriver.Chrome(ChromeDriverManager().install())

    @staticmethod
    def _add_queries(endpoint: str, queries: dict):
        """for decorating a URL with PHP query requests."""
        i = 0
        for k, v in queries.items():
            i += 1
            c = "?" if i < 2 else "&"
            endpoint += f"{c}{k}={v}"

        return endpoint

    def open_trends(self, *args):
        """"download google trend data for any number of terms and append to local CSV"""
        DL_FP = r'C:\Users\hunte\Downloads\multiTimeline.csv'
        concatenated = False
        tickers = args  # make tuple in to comma separated list
        ticker_bucket = [','.join(tickers[i:i+5]) for i in range(0, len(tickers), 5)]  # format tickers for URL

        # delete old MultiTimeline.csv if it exists in downloads
        for q in ticker_bucket:
            if os.path.isfile(DL_FP):
                os.remove(DL_FP)
    
            # download...
            self.driver.get('https://trends.google.com')  # load the web page first... something about a cookie 429...
            url = self._add_queries(f'https://trends.google.com/trends/explore', {'geo': 'US', 'q': q})
            self.driver.get(url)
            time.sleep(1)  # lol
            self.driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/md-content/div/div/div[1]/trends-widget/ng-'
                                              'include/widget/div/div/div/widget-actions/div/button[1]').click()
            time.sleep(1)

            # when it's downloaded...
            if os.path.isfile(DL_FP):
                trend_cleaner(DL_FP)  # this puts it in the cache
                new_trends = pd.read_csv("data/dynamic/trend/multiTimeline.csv")
                old_trends = pd.read_csv(Scraper.TRENDS_FP)
                concatenated = old_trends.merge(new_trends, on='Week', how='left')  # pretty sure this doesn't work
            concatenated.to_csv(Scraper.TRENDS_FP)
            print('concatenated dataframes.')
        # remove_false_index()  # this is literally a bug
        self.driver.close()

    def google(self, value: str):
        search_string = value.replace(' ', '+')
        self.driver.get(f"https://www.google.com/search?q=" + search_string)


# because i merger the df's incorrectly
def remove_false_index():
    trends_dirty = pd.read_csv(Scraper.TRENDS_FP, index_col="Week")
    for col in trends_dirty:
        if 'Unnamed' in col:
            trends_dirty.drop(col, axis=1, inplace=True)
    trends_dirty.to_csv(Scraper.TRENDS_FP)


if __name__ == "__main__":
    sc = Scraper()
    sc.open_trends('GHIV')
