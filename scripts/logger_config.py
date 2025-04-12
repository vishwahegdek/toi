import logging
import configparser

def get_logger(__name__):
    config = configparser.ConfigParser(allow_no_value=True)
    config.read("config.ini")
    
    # Create a custom logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Set the logger level to the lowest level you want to capture

    # Create handlers for both INFO and DEBUG levels
    info_handler = logging.FileHandler(config.get("logger", "debug_log"))
    info_handler.setLevel(logging.INFO)

    debug_handler = logging.FileHandler(config.get("logger", "info_log"))
    debug_handler.setLevel(logging.DEBUG)

    # Create formatters and add them to the handlers
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    info_handler.setFormatter(formatter)
    debug_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(info_handler)
    logger.addHandler(debug_handler)

    # Log messages
    return logger

