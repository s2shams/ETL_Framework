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
    log_memory_usage,
    load_ndjson_to_BQ
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
    INGEST_FLOW_NAME,
    TEMP_FILE,
    LOG_FREQUENCY,
    TABLE_ID,
    LOAD_CONFIG
)

from gcs_writer import Gcs_writer

# ------------- ETL specific imports --------------

# parse command line arguments
parser.add_argument('--target', '-t', type=str, default='dev', choices=['dev', 'prod'], help='Target environment for the ETL run')
parser.add_argument('--start_time', type=str, default=None, help='Optional start time in either ISO8601 or epoch')
parser.add_argument('--end_time', type=str, default=None, help='Optional end time in either ISO8601 or epoch')
args = parser.parse_args()

# Function to ingest data for a single subreddit given a time range
def ingest_subreddit(subreddit, start_time, end_time, loop_counter, writer):
    after = start_time
    before = end_time

    while True:
        request = build_posts_request(after, before, subreddit)
        response = make_api_request(request, logger=logger)
        batch_data, after = process_response_json(response, logger=logger)

        if not batch_data:
            logger.info(f'No more posts to ingest for subreddit: {subreddit}')
            break
        
        loop_counter += 1
        if loop_counter > 0 and loop_counter % LOG_FREQUENCY == 0:
            log_memory_usage(f"After {loop_counter} API calls")

        writer.write_batch(batch_data)
        return loop_counter

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

    loop_counter = 0
    writer = Gcs_writer(TEMP_FILE)

    logger.info(f"Subreddits being ingested: {SUBREDDITS}")
    for subreddit in SUBREDDITS:
        loop_counter = ingest_subreddit(subreddit, start_time, end_time, loop_counter, writer)
    
    writer.close()
    logger.info("All subreddits have been ingested. Proceeding to final chunk if present")
    if os.path.exists(TEMP_FILE) and os.path.getsize(TEMP_FILE) > 0:
        load_ndjson_to_BQ(TEMP_FILE, TABLE_ID, LOAD_CONFIG)
        os.remove(TEMP_FILE)

ingest_reddit()
    
    
