import json
import boto3

# Import ajustado para Lambda
try:
    from src.utils.logger import get_logger
except ImportError:
    from utils.logger import get_logger

logger = get_logger(__name__)

# ...existing code...