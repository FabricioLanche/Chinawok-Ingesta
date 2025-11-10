import json
import boto3
from src.utils.logger import get_logger

logger = get_logger(__name__)

s3 = boto3.client('s3')

def upload_to_s3(bucket, key, data):
    """
    Sube datos JSON a S3
    
    Args:
        bucket (str): Nombre del bucket S3
        key (str): Key del objeto en S3
        data (dict): Datos a subir
    """
    try:
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json',
            ServerSideEncryption='AES256'
        )
        
        logger.info(f'Archivo subido exitosamente a s3://{bucket}/{key}')
        
    except Exception as e:
        logger.error(f'Error subiendo a S3: {str(e)}')
        raise
