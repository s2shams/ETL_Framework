import os
import sys
import json
from google.cloud import bigquery

# Set working directory
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.etlmodules import log_memory_usage, load_ndjson_to_BQ
from utils.etllogger import get_logger
from config import TABLE_ID, LOAD_CONFIG

logger = get_logger(__name__)

class Gcs_writer:
    def __init__(self, path):
        self.path = path
        self.file = open(path, 'a', encoding='utf-8')

    def close(self):
        if self.file and not self.file.closed:
            self.file.close()
    
    def should_rotate(self):
        if log_memory_usage(None, silent=True) != 'normal':
            logger.warning("Memory warning detected. Proceeding to rotate the file")
            return True
        else:
            return False
    
    def write_batch(self, batch_data):
        if self.should_rotate():
            self.rotate()
        
        for post in batch_data:
            self.file.write(json.dumps(post) + '\n')
    
    def rotate(self):
        self.file.close()

        job_config = LOAD_CONFIG
        load_ndjson_to_BQ(self.path, TABLE_ID, job_config)
        os.remove(self.path)

        self.file = open(self.path, 'a', encoding='utf-8')