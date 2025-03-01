"""
Test the logging system.
"""
from src.utils.logger import log

def test_logger():
    """Test basic logging functionality."""
    log.debug("Debug message")
    log.info("Info message")
    log.warning("Warning message")
    try:
        raise ValueError("Test error")
    except Exception as e:
        log.error(f"Error message: {str(e)}")
        log.exception("Exception details")
