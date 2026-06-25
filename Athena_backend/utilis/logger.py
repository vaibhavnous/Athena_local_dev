import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from logtail import LogtailHandler

DEFAULT_PIPELINE_LOG_PATH = (
    Path("/home/site/logs/pipeline_logs.json")
    if Path("/home/site").exists()
    else Path(__file__).resolve().parents[1] / "pipeline_logs.json"
)

PIPELINE_LOG_PATH = Path(
    os.environ.get("ATHENA_PIPELINE_LOG_FILE", DEFAULT_PIPELINE_LOG_PATH)
).resolve()


class AthenaJsonFormatter(logging.Formatter):
    """Enhanced JSON formatter that captures custom structured fields."""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
        }

        standard_attrs = {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "message",
            "module",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
        }

        for key, value in record.__dict__.items():
            if key not in standard_attrs:
                log_entry[key] = value

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


class ConsoleContextFormatter(logging.Formatter):
    """Makes terminal logs readable by prefixing the LangGraph node name."""

    def format(self, record):
        node = getattr(record, "node", "SYSTEM")
        return f"{self.formatTime(record, self.datefmt)} | {record.levelname:^7} | [{node}] {record.getMessage()}"


def get_athena_logger():
    logger = logging.getLogger("athena")
    logger.propagate = False

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        logtail_token = os.environ.get("LOGTAIL_TOKEN", "").strip()
        if logtail_token:
            logtail_handler = LogtailHandler(source_token=logtail_token)
            logger.addHandler(logtail_handler)

        suppress_console = os.environ.get("ATHENA_SUPPRESS_CONSOLE", "").strip().lower() in {"1", "true", "yes", "on"}
        if not suppress_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(ConsoleContextFormatter())
            logger.addHandler(console_handler)

        PIPELINE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        json_handler = logging.FileHandler(PIPELINE_LOG_PATH, encoding="utf-8")
        json_handler.setFormatter(AthenaJsonFormatter())
        logger.addHandler(json_handler)

    return logger


logger = get_athena_logger()
