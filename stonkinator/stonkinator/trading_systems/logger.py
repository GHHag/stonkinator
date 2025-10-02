import os
import logging
from logging.handlers import TimedRotatingFileHandler

def create_timed_rotating_logger(log_dir: str, name: str, interval: int, backup_count: int) -> logging.Logger:
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler(
        filename=f"{log_dir}{name}.log",
        when="midnight",
        interval=interval,
        backupCount=backup_count,
        encoding="utf-8",
        utc=True
    )
    handler.suffix = "%Y-%m-%d"
    logging_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(logging_formatter)
    logger.addHandler(handler)

    return logger