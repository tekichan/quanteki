# core modules
import logging

# Define Logging attributes
def getLogger(
    logger_name
    , logger_level = 'INFO'
    ):
    FORMAT = '%(asctime)-15s,%(levelname)s:%(name)s %(message)s'
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_level)
    return logger