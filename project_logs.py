import logging
import os

try:
    os.mkdir("projectlogs")
except Exception as e:
    pass


class MyLogger:
    def __init__(self, filename='./projectlogs/project.log', level=logging.DEBUG):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Create a file handler and set the formatter
        file_handler = logging.FileHandler(filename)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def log(self, message, level=logging.INFO):
        if level == logging.DEBUG:
            self.logger.debug(message)
        elif level == logging.INFO:
            self.logger.info(message)
        elif level == logging.WARNING:
            self.logger.warning(message)
        elif level == logging.ERROR:
            self.logger.error(message)
        elif level == logging.CRITICAL:
            self.logger.critical(message)

    def log_execution(self,logger):
        def decorator(func):
            def wrapper(*args, **kwargs):
                try:
                    result = func(*args, **kwargs)
                    logger.log(message=f"Function: {func.__name__} completed successfully.", level=logging.INFO)
                    return result
                except Exception as e:
                    logger.log(message=f"Error in function: {func.__name__}: {e}", level=logging.ERROR)
                    raise e

            return wrapper

        return decorator


