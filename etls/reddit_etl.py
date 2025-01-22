import sys
import pandas as pd
import numpy as np
from praw import Reddit
from utils.constants import POST_FIELDS


def connect_reddit(client_id, secret, user_agent) -> Reddit:
    try:
        instance = Reddit(
            client_id=client_id,
            client_secret=secret,
            user_agent=user_agent
        )
        print('Connected to Reddit')
        return instance
    except Exception as e:
        print(e)
        sys.exit(1)


def extract_posts(instance: Reddit, sub_reddit, time_filter, limit):
    subreddit = instance.subreddit(sub_reddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_list = []

    for post in posts:
        post_dict = vars(post)
        post_list.append({key: post_dict[key] for key in POST_FIELDS})
    return post_list


def transform_data(df):
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
    df['over_18'] = np.where((df['over_18'] == True), True, False)
    df['author'] = df['author'].astype(str)
    edited_mode = df['edited'].mode()
    df['edited'] = np.where(df['edited'].isin([True, False]),
                                 df['edited'], edited_mode).astype(bool)
    df['num_comments'] = df['num_comments'].astype(int)
    df['score'] = df['score'].astype(int)
    df['title'] = df['title'].astype(str)

    return df


def load_to_csv(df: pd.DataFrame, path):
    df.to_csv(path, index=False)
# ins = connect_reddit('QXQO5i6s1ADOqe5sAm60mg', 'q0g4huR76MeIFvuZIPt1yEyPUJhoEQ', "script:my_reddit_bot:v1.0 (by u/Ill_Guarantee_5859)")
# extract_posts(ins, 'dataengineering', 'day', 10)


