import boto3
from decimal import Decimal

# Import ajustado para Lambda
try:
    from src.utils.logger import get_logger
except ImportError:
    from utils.logger import get_logger

logger = get_logger(__name__)

# ...existing code...