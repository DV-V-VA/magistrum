import logging
import logging.config
from pathlib import Path


def setup_logging(log_path: str | Path | None = None):
    if log_path is not None:
        log_path = Path(log_path)
        log_path.parent.mkdir(exist_ok=True)

        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "[%(asctime)s]-[%(name)s]-[%(levelname)s]: %(message)s"
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                    "level": "INFO",
                },
                "file": {
                    "class": "logging.FileHandler",
                    "filename": log_path,
                    "formatter": "standard",
                    "level": "DEBUG",
                },
            },
            "root": {
                "handlers": ["console", "file"],
                "level": "DEBUG",
            },
        }
        logging.config.dictConfig(logging_config)
    else:
        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "[%(asctime)s]-[%(name)s]-[%(levelname)s]: %(message)s"
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                    "level": "INFO",
                },
            },
            "root": {
                "handlers": ["console"],
                "level": "DEBUG",
            },
        }
        logging.config.dictConfig(logging_config)
