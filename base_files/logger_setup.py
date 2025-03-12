import logging
import os

# Define log directory
LOG_DIR = '/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/ETL_workflow/logs_files/'

# Ensure the log directory exists
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logger(name, log_file=None):
    """
    Sets up and returns a logger with file and console handlers.

    Args:
        name (str): Logger name.
        log_file (str): Log file path.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Default log file path
    if log_file is None:
        log_file = os.path.join(LOG_DIR, f"{name}.log")

    # File handler for writing logs to a file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Stream handler for logging to the console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Add handlers to the logger
    if not logger.handlers:  # Prevent adding duplicate handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    # Prevent log propagation to the root logger
    logger.propagate = False

    return logger
