"""
Global settings and configuration for the quantitative trading system.
"""
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Base Paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "cache"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# Data Source Configuration
DATA_SOURCE_CONFIG: Dict[str, Any] = {
    "akshare": {
        "retry_count": 3,
        "retry_delay": 1,
        "timeout": 30,
    },
    "futu": {
        "host": os.getenv("FUTU_HOST", "127.0.0.1"),
        "port": int(os.getenv("FUTU_PORT", "11111")),
        "retry_count": 3,
    }
}

# Storage Configuration
STORAGE_CONFIG = {
    "format": "parquet",
    "compression": "snappy",
    "partition_cols": ["market", "date"],
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    "rotation": "1 week",
}

# Market Trading Hours (UTC+8)
MARKET_HOURS = {
    "CN": {  # A-shares
        "morning": ("09:30", "11:30"),
        "afternoon": ("13:00", "15:00"),
        "trading_days": "1-5",  # Monday to Friday
    },
    "HK": {  # Hong Kong
        "morning": ("09:30", "12:00"),
        "afternoon": ("13:00", "16:00"),
        "trading_days": "1-5",
    }
}
