import logging

formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)-8s [%(module)s/%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a logger.

    Args:
        name: logger name
        log_file: logging file
        level: logging level

    Returns:
        New logger.
    """
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def get_local_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a logger to be used locally (not tracked in GIT)

    Args:
        name: logger name
        level: logging level

    Returns:
        Local logger.
    """
    return setup_logger('local_' + name, 'logs_local.txt', level)


def get_download_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a logger for download-related events (tracked in GIT)

    Args:
        name: logger name
        level: logging level

    Returns:
        Local logger.
    """
    return setup_logger('download_' + name, 'logs_download.txt', level)
