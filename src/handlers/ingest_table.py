import json
import os
from datetime import datetime
from src.utils.dynamodb_client import get_table_data
from src.utils.s3_client import upload_to_s3
from src.utils.logger import get_logger

logger = get_logger(__name__)

def handler(event, context):
    # ...codigo existente...
    pass