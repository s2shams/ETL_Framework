import json
from config import POSTS_API_URL, COMMENTS_API_URL, POST_FIELDS, COMMENT_FIELDS, POST_BATCH_SIZE, COMMENT_BATCH_SIZE, MAX_API_RETRIES
import requests
from datetime import datetime, timezone
import time
import random
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

# Get reddit posts from API with pagination parameters (after, before) and subreddit name
def build_posts_request(after, before, subreddit, limit=POST_BATCH_SIZE, url=POSTS_API_URL, fields=POST_FIELDS):
    request_url = url + f'?sort=asc&after={after}&before={before}&subreddit={subreddit}&limit={limit}&fields={",".join(fields)}'
    return request_url

# Get reddit comments for a post
# ** ASSUMPTION: we will get all comments for a post in one request, so no pagination parameters needed**
def build_comments_request(post_id, url=COMMENTS_API_URL, limit=COMMENT_BATCH_SIZE):
    request_url = url + f'?link_id={post_id}&limit={limit}'
    return request_url

# Used for api pagination to get the next batch of posts/comments
# May not need to use, can directly use the epoch time of last post to get, but may be useful for logging
def convert_epoch_to_ISO8601(epoch_time):
    dt_utc = datetime.fromtimestamp(epoch_time, tz=timezone.utc)
    iso_string = dt_utc.isoformat()
    return iso_string

# Make API request with retries and exponential backoff
def make_api_request(url, max_retries=MAX_API_RETRIES, logger=None):
    base_delay = 1 # Base delay in seconds for exponential backoff
    max_delay = 60 # Max delay in seconds for exponential backoff

    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status() # Raise an exception for HTTP errors
            return response.json() # Return the JSON response if successful
        except requests.exceptions.RequestException as e:
            delay = min(max_delay, base_delay * (2 ** attempt))
            sleep_time = random.uniform(0, delay) # Add jitter to avoid thundering herd problem

            if logger:
                logger.warning(f'API request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {sleep_time:.2f} seconds...')
            else:
                print(f'API request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {sleep_time:.2f} seconds...')
            time.sleep(sleep_time)
    # If we exhaust all retries, raise an exception
    raise Exception(f'API request failed after {max_retries} attempts: {url}')

# Calculate sentiment score for a given text using VADER sentiment analyzer
def calculate_sentiment(text):
    if not text:
        return None
    sentiment_scores = analyzer.polarity_scores(text)['compound']
    return sentiment_scores

# Process a comment by recursively processing its replies and pulling fields that we want
def process_comment(comment, logger=None, level=0):
    data = comment.get('data', {})

    # base case: if comment has no data, return
    if not data:
        return {}
    
    # pull fields that we want from the comment data
    processed_comment = {field: data.get(field) for field in COMMENT_FIELDS}
    processed_comment['depth'] = level # add depth field to keep track of how deep in the comment tree we are, with top-level comments having depth 0
    processed_comment['sentiment_score'] = calculate_sentiment(data.get('body', '')) # calculate sentiment score for the comment body and add to comment data

    # recursively process replies if they exist
    processed_comment.get('replies', [])
    
    # if a top-level comment has no replies, the API returns an empty string
    if isinstance(processed_comment['replies'], str):
        processed_comment['replies'] = None
    # if a comment has replies, it is stored as a dict which contains a 'data.children' field which is a list of replies with the same structure as comments
    elif isinstance(processed_comment['replies'], dict):
        replies_data = processed_comment['replies'].get('data', {}).get('children', [])
        processed_replies = [process_comment(reply, logger=logger, level=level+1) for reply in replies_data]
        processed_comment['replies'] = processed_replies
    return processed_comment

# Process the response JSON to extract relevant fields and structure the data as needed for our database schema
def process_response_json(response_json, logger=None):
    data = response_json.get('data', [])
    
    for post in data:

        # for each post, get the comments and add to the post data
        post_id = post.get('id')
        comment_request_url = build_comments_request(post_id)
        try:
            comment_response_json = make_api_request(comment_request_url, logger=logger)
            comment_data = comment_response_json.get('data', [])

            # process each comment first by recursively processing replies and pulling fields that we want
            processed_comments = [process_comment(comment, logger=logger, level=0) for comment in comment_data] or None
            
            post['comments'] = processed_comments

            # Calculate sentiment score for the post title + selftext and add to post data
            post_text = post.get('title', '') + ' ' + post.get('selftext', '')
            post['sentiment_score'] = calculate_sentiment(post_text)
        except Exception as e:
            if logger:
                logger.error(f'Failed to get comments for post {post_id}: {e}')
            else:
                print(f'Failed to get comments for post {post_id}: {e}')
            post['comments'] = None # Set comments to None if we fail to get comments for a post
            continue
    
    # get the created_utc of the last post for pagination
    last_post_created_utc = None
    if data:
        last_post = data[-1]
        last_post_created_utc = last_post.get('created_utc')

    return data, last_post_created_utc
    
## TEST CODE ##

post_url = build_posts_request(after='2026-05-01T00:00:00Z', before='2026-05-07T00:00:00Z', subreddit='uwaterloo')
response = make_api_request(post_url)
batch_data, _ = process_response_json(response)

with open('temp_response.json', 'w') as f:
    json.dump(batch_data, f, indent=4)