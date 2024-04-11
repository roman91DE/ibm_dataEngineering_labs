import logging


def get_logger(name: str, log_path: str) -> logging.Logger:
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    file_handler: logging.Handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    stream_handler: logging.Handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    for handler in (file_handler, stream_handler):
        logger.addHandler(handler)

    return logger
