from .logger_config import get_logger, setup_logger

logger = get_logger(__name__)


def hello() -> str:
    message = "Hello from py-adsb-historical-data-client!"
    logger.info(message)
    return message


__all__ = ["hello", "setup_logger", "get_logger"]
