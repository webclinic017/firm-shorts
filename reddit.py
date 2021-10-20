import datetime
import praw
import json
import pandas as pd
import pyarrow.feather as feather

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
        feather.write_feather(post_df, 'data/dynamic/reddit/hot_posts.feather')
        feather.write_feather(comment_df, 'data/dynamic/reddit/hot_comments.feather')
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
            print(f'on post {self.num_posts_searched}: {post.title}')
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
        post_df = feather.read_feather('data/dynamic/reddit/hot_posts.feather')
        comment_df = feather.read_feather('data/dynamic/reddit/hot_comments.feather')

        self.num_comments_searched = len(post_df)
        self.num_posts_searched = len(comment_df)

        return post_df, comment_df


# MAIN
lurker = Lurker('wallstreetbets')
start = datetime.datetime.now()
ps, cs = lurker.download_hot(post_limit=1, comment_limit=1)
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
