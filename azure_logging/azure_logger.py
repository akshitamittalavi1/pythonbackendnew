import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler

def setup_logger():
    
    log_file = "log.txt"

    # Configure the logging settings
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Create a file handler to write logs to the JSON file
    file_handler = logging.FileHandler(log_file)

    # Create a formatter for the log records
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Set the formatter for the file handler
    file_handler.setFormatter(formatter)

    # Create a logger and add the file handler to it
    logger = logging.getLogger(__name__)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()
