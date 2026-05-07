import json
import logging
import sys
from datetime import datetime, timezone

def success(self, msg, *args, **kwargs):
    self.info(f"SUCCESS: {msg}", *args, **kwargs)

def failure(self, msg, *args, **kwargs):
    self.error(f"FAILURE: {msg}", *args, **kwargs)

# Configure logging
logging.Logger.success = success
logging.Logger.failure = failure

class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)

def get_logger(name="app", level="INFO", json_logs=True):
    # initialize logger
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level.upper())

    # remove handlers to avoid duplicate logs
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    # create stream handler to output logs to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level.upper())

    # set formatter to JSON or plain text
    if json_logs:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger