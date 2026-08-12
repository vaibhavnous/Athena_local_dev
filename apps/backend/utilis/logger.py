import json
import logging
import os
import re
import sys
from collections.abc import Mapping
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

_SENSITIVE_KEY = re.compile(
    r"(?i)(authorization|password|passwd|pwd|token|secret|private[_ -]?key|client[_ -]?secret|"
    r"access[_ -]?key|api[_ -]?key|sas|credential|signature|sig)"
)


def redact_sensitive_text(value, limit=None):
    text = str(value or "")
    text = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^/@\s:]+:[^/@\s]+@", r"\1[REDACTED]@", text)
    text = re.sub(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED]", text)
    text = re.sub(
        r'(?i)(authorization|password|passwd|pwd|token|secret|private[_ -]?key|client[_ -]?secret|'
        r'access[_ -]?key|api[_ -]?key|sas|credential|signature|sig)'
        r'(["\']?\s*[:=]\s*["\']?)[^"\'\s,;}&]+',
        r"\1\2[REDACTED]",
        text,
    )
    return text if limit is None else text[:limit]


def redact_sensitive(value, *, key=""):
    if isinstance(value, Mapping):
        reference_keys = {str(name).casefold() for name in value}
        if reference_keys == {"scope", "key"} and all(str(item or "").strip() for item in value.values()):
            return {str(name): item for name, item in value.items()}
        if _SENSITIVE_KEY.search(str(key)) and value and all(
            isinstance(item, Mapping)
            and {str(name).casefold() for name in item} == {"scope", "key"}
            for item in value.values()
        ):
            return {str(name): redact_sensitive(item) for name, item in value.items()}
    if _SENSITIVE_KEY.search(str(key)):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(name): redact_sensitive(item, key=str(name)) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, str):
        return redact_sensitive_text(value)
    return value


class SecretRedactionFilter(logging.Filter):
    """Redact messages, structured fields and tracebacks before any handler sees them."""

    def filter(self, record):
        record.msg = redact_sensitive_text(record.getMessage())
        record.args = ()
        for key, value in tuple(record.__dict__.items()):
            if key not in {"msg", "args", "exc_info", "exc_text"}:
                record.__dict__[key] = redact_sensitive(value, key=key)
        if record.exc_info:
            record.exc_text = redact_sensitive_text(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        return True


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
            log_entry["exception"] = redact_sensitive_text(self.formatException(record.exc_info))
        elif record.exc_text:
            log_entry["exception"] = record.exc_text

        return json.dumps(log_entry)


class ConsoleContextFormatter(logging.Formatter):
    """Makes terminal logs readable by prefixing the LangGraph node name."""

    def format(self, record):
        node = getattr(record, "node", "SYSTEM")
        return f"{self.formatTime(record, self.datefmt)} | {record.levelname:^7} | [{node}] {redact_sensitive_text(record.getMessage())}"


def get_athena_logger():
    logger = logging.getLogger("athena")
    logger.propagate = False

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logger.addFilter(SecretRedactionFilter())

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
        try:
            json_handler = logging.FileHandler(PIPELINE_LOG_PATH, encoding="utf-8")
        except PermissionError:
            fallback_path = PIPELINE_LOG_PATH.with_name(f"{PIPELINE_LOG_PATH.stem}.{os.getpid()}{PIPELINE_LOG_PATH.suffix}")
            json_handler = logging.FileHandler(fallback_path, encoding="utf-8")
        json_handler.setFormatter(AthenaJsonFormatter())
        logger.addHandler(json_handler)

    return logger


logger = get_athena_logger()
