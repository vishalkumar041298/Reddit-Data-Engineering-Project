import os
import pandas as pd
from utils.constants import CLIENT_ID, SECRET, FILE_OUTPUT_PATH
from etls.reddit_etl import (
    connect_reddit, extract_posts, transform_data, load_to_csv
)

def reddit_pipeline(filename, sub_reddit, time_filter, limit):
    
    instance = connect_reddit(CLIENT_ID, SECRET, "script:my_reddit_bot:v1.0 (by u/Ill_Guarantee_5859)")
    posts = extract_posts(instance, sub_reddit, time_filter, limit)
    posts_df = pd.DataFrame(posts)

    posts_df = transform_data(posts_df)
    opath = os.path.join(FILE_OUTPUT_PATH, filename+'.csv')

    load_to_csv(posts_df, opath)

    return opath

