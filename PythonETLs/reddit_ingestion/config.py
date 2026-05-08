# Config for reddit ingestion

# Subreddit(s) to ingest
SUBREDDITS = [
    'uwaterloo',
    'Genshin_Impact',
    'XiaoMains',
    'ArlecchinoMains',
    'HollowKnight',
    'Kingdom'
]

# Base API URL for reddit posts
POSTS_API_URL = 'https://arctic-shift.photon-reddit.com/api/posts/search'

# Base API URL for reddit comments (tree endpoint to get all comments for a post)
COMMENTS_API_URL = 'https://arctic-shift.photon-reddit.com/api/comments/tree'

# Fields to extract from reddit posts and comments
POST_FIELDS = [
    'id',
    'subreddit',
    'subreddit_id',
    'author',
    'created_utc', # in epoch time
    'title',
    'selftext',
]

COMMENT_FIELDS = [

]

# Schema mapping for final BigQuery table
# tbd