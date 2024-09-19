import logging
from logging.handlers import RotatingFileHandler
import os


class UpdateLogger:
    def __init__(self, log_dir, base_filename, max_size_mb=5, backup_count=5):
        self.log_dir = log_dir
        self.base_filename = base_filename
        self.max_size_mb = max_size_mb
        self.backup_count = backup_count
        self._ensure_log_directory()
        self.logger = self._setup_logger()

    def _ensure_log_directory(self):
        """Ensure the log directory exists."""
        os.makedirs(self.log_dir, exist_ok=True)

    def _setup_logger(self):
        """Set up the logger with rotating file handler."""
        logger = logging.getLogger("UpdateLogger")
        logger.setLevel(logging.INFO)  # Set the logging level to INFO

        # Create a rotating file handler
        handler = RotatingFileHandler(
            self._get_log_file_path(),
            maxBytes=self.max_size_mb * 1024 * 1024,  # Max size in bytes
            backupCount=self.backup_count  # Number of backup files to keep
        )
        handler.setLevel(logging.INFO)

        # Create a formatter and set it for the handler
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(handler)
        return logger

    def _get_log_file_path(self):
        """Get the path for the log file."""
        return os.path.join(self.log_dir, f"{self.base_filename}.txt")

    def log_message(self, message):
        """Log a general message."""
        self.logger.info(message)

    def log_error(self, error_message):
        """Log an error message."""
        self.logger.error(error_message)
