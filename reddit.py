import datetime

import praw
import json
import pandas as pd
import feather

# screener imports
import pandas_datareader as data
import yfinance as yf
from datetime import date
yf.pdr_override()  # i forget what this does

# CONSTANTS
with open('secrets.json') as f:
    ALL_SECRETS = json.load(f)
    SECRET = ALL_SECRETS['PRAW API key']
    ID = ALL_SECRETS['PRAW ID']


class Lurker:
    """class using PRAW to collect and store financial data across subreddits
    :returns a lot of Pandas dataframes to be plotted and analyzed"""
    reddit = praw.Reddit(client_id=ID, client_secret=SECRET, user_agent='mubs')

    def __init__(self, subreddit='wallstreetbets'):
        self.sub = self.reddit.subreddit(subreddit)  # DEFAULT SUB WALLSTREETBETS
        self.num_comments_searched = 0
        self.num_posts_searched = 0
        self.metadata = {'date': datetime.datetime.now().strftime('%a %Y-%m-%d %H:%M'),
                         'posts searched': 0, 'comments searched': 0, 'notes': ''}

    def update_metadata(self, md=None):
        if md:
            self.metadata.update(md)

        self.metadata['posts searched'] = self.num_posts_searched
        self.metadata['comments searched'] = self.num_comments_searched

        with open('data/dynamic/reddit/metadata.json', 'w') as m:
            json.dump(self.metadata, m)

    def save_feather(self, post_df, comment_df):
        """store files using feather"""
        feather.write_dataframe(post_df, 'data/dynamic/reddit/hot_posts.feather')
        feather.write_dataframe(comment_df, 'data/dynamic/reddit/hot_comments.feather')
        self.update_metadata()

    def download_hot(self, post_limit=100, comment_limit=0) -> (pd.DataFrame, pd.DataFrame):
        """search hot posts & comments for a list of strings
        :returns two pandas dataframes containing data about the posts & comments respectively"""
        # Initialize pandas dataframes for storing reddit posts and columns
        post_df = pd.DataFrame(
            columns=['body', 'title', 'permalink', 'created_utc', 'num_comments', 'score', 'author', 'id'])
        comment_df = pd.DataFrame(
            columns=['body', 'permalink', 'created_utc', 'num_replies', 'score', 'author', 'id', 'post_id'])

        # START ITERATING THROUGH HOT POSTS
        for post in self.sub.hot(limit=post_limit):
            self.num_posts_searched += 1

            # ADD POST TO DATAFRAME
            # Deal with deleted accounts first
            try:
                name = post.author.name
            except AttributeError:
                name = "None"

            # add post row to dataframe
            post_df.loc[len(post_df.index)] = [post.selftext, post.title, post.permalink,
                                               post.created_utc, post.num_comments,
                                               post.score, name, post.id]

            # need to use MoreComments object which needs to be imported idrk
            post.comments.replace_more(limit=comment_limit)  # THIS IS IMPORTANT

            # ITERATE THROUGH COMMENTS
            for comment in post.comments.list():  # list method flattens the comment forest ig
                self.num_comments_searched += 1  # counter

                # check for deleted accounts
                try:
                    name = comment.author.name
                except AttributeError:
                    name = "None"

                # add comment row to dataframe
                comment_df.loc[len(comment_df)] = [comment.body, comment.permalink, comment.created_utc,
                                                   len(comment.replies), comment.score, name,
                                                   comment.id, comment.submission.id]

        self.save_feather(post_df, comment_df)
        return post_df, comment_df

    def open_hot(self) -> (pd.DataFrame, pd.DataFrame):
        post_df = feather.read_dataframe('data/dynamic/reddit/hot_posts.feather')
        comment_df = feather.read_dataframe('data/dynamic/reddit/hot_comments.feather')
        print('SAVED TO data/dynamic/reddit/')

        self.num_comments_searched = len(post_df)
        self.num_posts_searched = len(comment_df)

        return post_df, comment_df

    @staticmethod
    def unusual_activity(calls_or_puts, exp_date, stocklist):
        """
        unusualActivity scans yahoo finance for a list of stocks and returns contracts showing unusually high
        volume/open interest

        Args:
        calls_or_puts (str): do you want to return calls or puts?
        exp_date (str): date of contract expiry
        stocklist (list[str]): list of tickers to loop through
        """
        # We're going to suppress prints b/c datareader is annoying then restore printing so this helps
        finaldf = pd.DataFrame()
        for x in stocklist:
            ticker = yf.Ticker(x)
            print(ticker.ticker)
            try:
                opt = ticker.option_chain(exp_date)

                # NEED TO CONSOLIDATE THIS IF STATEMENT '-_-
                # why would anyone do this
                if calls_or_puts == 'calls':
                    # Add some info about ticker and UL price to our dataframe
                    opt.calls.insert(0, 'Symbol', x)
                    opt.calls.insert(3, 'stock_price', data.get_data_yahoo(x, end=date.today())['Close'][-1])
                    # Calculate our volume/open interest, this is how we define unusual activity
                    opt.calls['V/OI'] = (opt.calls['volume'].astype('float') / opt.calls['openInterest'])
                    # Inside of the brackets is where we apply our filter to get the unusual stuff.
                    # Feel free to mess with this in your own version
                    finaldf = finaldf.append(opt.calls[(opt.calls['volume'].astype('float') / opt.calls[
                        'openInterest'] > .5) & (opt.calls['openInterest'] > 200)])
                elif calls_or_puts == 'puts':
                    # sys.stdout = open(os.devnull, "w")
                    opt.puts.insert(0, 'Symbol', x)
                    opt.puts.insert(3, 'stock_price', data.get_data_yahoo(x, end=date.today())['Close'][-1])
                    opt.puts['stock_price'] = data.get_data_yahoo(x, end=date.today())['Close'][-1]
                    opt.puts['V/OI'] = (opt.puts['volume'].astype('float') / opt.puts['openInterest'])
                    finaldf = finaldf.append(opt.puts[(opt.puts['volume'].astype('float') / opt.puts[
                        'openInterest'] > .5) & (opt.puts['openInterest'] > 200)])
                else:
                    print('set calls_or_puts equal to calls or puts retard')
                    break
            except:
                print('cant get option chain')

        # final formatting changes
        finaldf = finaldf.drop(columns=['contractSymbol', 'lastTradeDate', 'contractSize', 'currency'])
        finaldf.insert(3, 'Distance OTM', finaldf['stock_price'] - finaldf['strike'])
        finaldf['Value'] = finaldf['openInterest'] * finaldf['lastPrice'] * 100

        return finaldf


# MAIN
lurker = Lurker('wallstreetbets')
start = datetime.datetime.now()
ps, cs = lurker.download_hot(post_limit=10, comment_limit=2)
print(f'runtime: {datetime.datetime.now()-start}')
# ps, cs = lurker.open_hot()
print(ps, cs)

# for i, post_id in enumerate(ps['id'][:25]):
#     comments_on_post = cs.loc[cs['post_id'] == post_id]
#     plt.subplot(5, 5, i+1)
#     print(comments_on_post)
#     comments_on_post['score'].plot(kind='kde')

# plt.legend()
# plt.show()

# expiry = '2021-09-24'
# returned = Lurker.unusual_activity('calls', expiry, sp_list)
# print(returned)
# print(returned['Value'])
# returned.to_csv(f'./data/dynamic/stock/market/unusual_options_activity_{expiry}.csv')
