# Config for reddit ingestion

# Subreddit(s) to ingest
SUBREDDITS = [
    'uwaterloo',
    'Genshin_Impact',
    'XiaoMains',
    'ArlecchinoMains',
    'HollowKnight',
    'Kingdom',
    'Ford'
]

# Base API URL for reddit posts
POSTS_API_URL = 'https://arctic-shift.photon-reddit.com/api/posts/search'
POST_BATCH_SIZE = 100 # Max batch size for reddit posts API

# Base API URL for reddit comments (tree endpoint to get all comments for a post)
COMMENTS_API_URL = 'https://arctic-shift.photon-reddit.com/api/comments/tree'
COMMENT_BATCH_SIZE = 9999 # Max batch size for reddit comments API

# Max retries for API requests
MAX_API_RETRIES = 5

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
# tbd