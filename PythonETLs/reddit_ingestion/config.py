# Config for reddit ingestion
from google.cloud import bigquery

# Subreddit(s) to ingest
SUBREDDITS = [
    'uwaterloo',
    'Genshin_Impact',
    'Genshin_Impact_Leaks',
    'anime',
    'manga',
    'Ford',
    'dataengineering',
    'learnmachinelearning',
    'canadaexpressentry',
    'ClashRoyale',
    'UofT',
    'waterloo',
    'askTO',
    'cscareerquestions',
    'kitchener',
    'hiringcafe',
    'jobs',
    'internships',
    'PersonalFinanceCanada',
    'Layoffs',
    'SipsTea',
    'ontario',
    'technology',
    'news'
]

INGEST_FLOW_NAME = 'reddit_ingestion'
LOAD_FLOW_NAME = 'reddit_staging'
MERGE_FLOW_NAME = 'reddit_merge'
TEMP_FILE = 'reddit_posts.ndjson'
LOG_FREQUENCY = 10

DATASET_NAME = 'common_property'
TABLE_ID = f'{DATASET_NAME}.reddit_data'
STAGING_TABLE_ID = f'{DATASET_NAME}_staging.reddit_data_staging'

DEFAULT_DAYS_AGO = 8

# Base API URL for reddit posts
POSTS_API_URL = 'https://arctic-shift.photon-reddit.com/api/posts/search'
POST_BATCH_SIZE = 100 # Max batch size for reddit posts API

# Base API URL for reddit comments (tree endpoint to get all comments for a post)
COMMENTS_API_URL = 'https://arctic-shift.photon-reddit.com/api/comments/tree'
COMMENT_BATCH_SIZE = 9999 # Max batch size for reddit comments API

# Max retries for API requests
MAX_API_RETRIES = 5

# Max Threads for concurrent API requests (per subreddit)
MAX_THREADS = 2

# Fields to extract from reddit posts and comments
POST_FIELDS = [
    'id',
    'subreddit',
    'subreddit_id',
    'author',
    'author_flair_text',
    'link_flair_text',
    'created_utc', # in epoch time
    'retrieved_on', # in epoch time, when we retrieved the post data from the API
    'title',
    'selftext'
]

# these are fields within comments.data, which is the structure of the reddit comments API response
COMMENT_FIELDS = [
    'id',
    'link_id', # post id that the comment is associated with
    'parent_id', # parent comment id, if it's a reply to another comment, or post id if it's a top-level comment
    'subreddit',
    'subreddit_id',
    'author',
    'author_flair_css_class',
    'author_flair_text',
    'created_utc', # in epoch time
    'retrieved_on', # in epoch time, when we retrieved the comment data from the API
    'body',
    'replies', # list of child comments (replies)
    'downs',
    'ups',
    'score',
]

# Schema mapping for final BigQuery table
SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("subreddit", "STRING"),
    bigquery.SchemaField("subreddit_id", "STRING"),
    bigquery.SchemaField("author", "STRING"),
    bigquery.SchemaField("author_flair_text", "STRING"),
    bigquery.SchemaField("link_flair_text", "STRING"),
    bigquery.SchemaField("created_utc", "TIMESTAMP"),
    bigquery.SchemaField("retrieved_on", "INTEGER"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("selftext", "STRING"),
    bigquery.SchemaField("sentiment_score", "FLOAT"),
    bigquery.SchemaField("comments", "RECORD", mode="REPEATED", fields= [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("link_id", "STRING"),
        bigquery.SchemaField("parent_id", "STRING"),
        bigquery.SchemaField("subreddit", "STRING"),
        bigquery.SchemaField("subreddit_id", "STRING"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("author_flair_css_class", "STRING"),
        bigquery.SchemaField("author_flair_text", "STRING"),
        bigquery.SchemaField("created_utc", "INTEGER"),
        bigquery.SchemaField("retrieved_on", "INTEGER"),
        bigquery.SchemaField("body", "STRING"),
        bigquery.SchemaField("downs", "INTEGER"),
        bigquery.SchemaField("ups", "INTEGER"),
        bigquery.SchemaField("score", "INTEGER"),
        bigquery.SchemaField("sentiment_score", "FLOAT"),
        bigquery.SchemaField("replies", "JSON", mode="REPEATED")
    ])
]

LOAD_CONFIG = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=SCHEMA,
            autodetect=False,
            ignore_unknown_values=True,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_utc"
            )
        )

