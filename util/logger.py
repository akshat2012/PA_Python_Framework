import logging


def create_logger():
    # Create custom Logger
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    # Create Handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('./file.log')

    c_handler.setLevel(logging.DEBUG)
    f_handler.setLevel(logging.INFO)

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Clear any pre-existing handlers from the logger
    # Add handlers to the logger
    # set propogate to False to disable root logger to log things
    logger.handlers.clear()
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    logger.propagate = False

    return logger
