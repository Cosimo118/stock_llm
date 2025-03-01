"""
Logging utility module for the quantitative trading system.
"""
from loguru import logger
import sys
from ..config.settings import LOGGING_CONFIG, BASE_DIR

def setup_logger():
    """Configure the logger with predefined settings."""
    # Remove default handler
    logger.remove()
    
    # Add custom handler for console output
    logger.add(
        sys.stderr,
        format=LOGGING_CONFIG["format"],
        level=LOGGING_CONFIG["level"],
        colorize=True
    )
    
    # Add file handler for persistent logs
    log_file = BASE_DIR / "logs" / "app.log"
    log_file.parent.mkdir(exist_ok=True)
    
    logger.add(
        str(log_file),
        rotation=LOGGING_CONFIG["rotation"],
        level=LOGGING_CONFIG["level"],
        format=LOGGING_CONFIG["format"],
        compression="zip"
    )
    
    return logger

# Create logger instance
log = setup_logger()
