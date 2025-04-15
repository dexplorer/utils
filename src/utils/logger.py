import logging
from logging.handlers import TimedRotatingFileHandler
from utils.enums import LogHandler
import sys


def config_logger(log_file_path_name):
    # log_file = f"./dqml_app/log/{log_file_name}.log"
    log_file = log_file_path_name
    logging.basicConfig(
        format="%(asctime)s : %(levelname)s : %(filename)s (%(lineno)d) : %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S %p",
        level=logging.INFO,
        filename=log_file,
        filemode="w",
        force=True,
    )
    logging.captureWarnings(True)
    # logging.FileHandler(filename, mode='a', encoding=None, delay=False)


def config_multi_platform_logger(
    log_level: int, handlers: list[str], log_file_path_name: str = ""
):
    log_record_format = (
        "%(asctime)s : %(levelname)s : %(filename)s (%(lineno)d) : %(message)s"
    )
    log_date_format = "%Y-%m-%d %I:%M:%S %p"

    log_handlers = []
    for handler in handlers:
        if handler == LogHandler.TIMED_ROTATING_FILE_HANDLER:
            log_handler = TimedRotatingFileHandler(
                filename=log_file_path_name,
                when="D",
                interval=7,
                backupCount=5,
                encoding=None,
                delay=False,
                utc=False,
            )
        elif handler == LogHandler.STREAM_HANDLER_STDOUT:
            log_handler = logging.StreamHandler(sys.stdout)
        else:
            log_handler = logging.StreamHandler()
        log_handlers.append(log_handler)

    logging.basicConfig(
        format=log_record_format,
        datefmt=log_date_format,
        level=log_level,
        # Remove existing handlers from root logger
        force=True,
        handlers=log_handlers,
    )
    logging.captureWarnings(True)
