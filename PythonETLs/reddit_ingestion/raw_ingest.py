# ------------- Common code --------------
from datetime import datetime, timedelta, timezone
import json
import sys
import os
import argparse

# Set working directory
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.etlmodules import (
    load_data_to_BQ,
    get_client,
    check_flow_exists,
    get_last_run_time,
    update_processing_status,
)
from utils.etllogger import get_logger

# Initialize logger
logger = get_logger('reddit_ingestion')
parser = argparse.ArgumentParser()
# ------------- Common code --------------

# ------------- ETL specific imports --------------
from api_utils import (
    build_posts_request,
    build_comments_request,
    convert_epoch_to_ISO8601,
    make_api_request,
    calculate_sentiment,
    process_comment,
    process_response_json
)

from config import (
    SUBREDDITS,
    MAX_THREADS,
    INGEST_FLOW_NAME
)

TEMP_FILE = 'reddit_posts.ndjson'
# ------------- ETL specific imports --------------

# parse command line arguments
parser.add_argument('--target', '-t', type=str, default='dev', choices=['dev', 'prod'], help='Target environment for the ETL run')
parser.add_argument('--start_time', type=str, default=None, help='Optional start time in either ISO8601 or epoch')
parser.add_argument('--end_time', type=str, default=None, help='Optional end time in either ISO8601 or epoch')
args = parser.parse_args()

# Function to ingest data for a single subreddit given a time range
def ingest_subreddit(subreddit, start_time, end_time, file_handle):
    after = start_time
    before = end_time
    temp_file = f'{subreddit}.ndjson' # temporary file to store raw json response from API for this subreddit

    while True:
        request = build_posts_request(after, before, subreddit)
        response = make_api_request(request, logger=logger)
        batch_data, after = process_response_json(response, logger=logger)

        if not batch_data:
            logger.info(f'No more posts to ingest for subreddit: {subreddit}')
            break

        for post in batch_data:
            file_handle.write(json.dumps(post) + '\n')
        
        file_handle.flush()

def ingest_reddit():
    lastETLdate = get_last_run_time(INGEST_FLOW_NAME)
    if lastETLdate is None:
        logger.info('No previous ETL run found, defaulting to ingesting post data from the start of the day 7 days prior to current date')
        lastETLdate = datetime.now(timezone.utc) - timedelta(days=7)
        lastETLdate = lastETLdate.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        logger.info(f'Previous ETL run found with last run time: {lastETLdate}.')
        lastETLdate = datetime.strptime(lastETLdate, '%Y-%m-%d %H:%M:%S%z').replace(tzinfo=timezone.utc)
    
    ETLcutofftime = lastETLdate + timedelta(days=1)

    # convert to correct format for API request
    start_time = lastETLdate.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = ETLcutofftime.strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info(f'Starting to ingest reddit data between time range: {start_time} to {end_time}')

    # open 1 temp file for all subreddits (maybe consider multiple files, but currently the data volume isn't high enough to justify)
    with open(TEMP_FILE, 'a') as f:
        # this will later be multithreaded (each thread writes to a queue managed by another thread, that thread writes to the temp file)
        for subreddit in SUBREDDITS:
            logger.info(f'Starting ingestion for subreddit: {subreddit}')
            ingest_subreddit(subreddit, start_time, end_time, f)
            logger.info(f'Finished ingestion for subreddit: {subreddit}')

ingest_reddit()
    
    
