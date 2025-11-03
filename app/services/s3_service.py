import boto3
import os
import sys
import json
from datetime import datetime
from typing import List, Dict, Any
import io
from app.config import settings


class S3Service:
    """Servicio para interactuar con S3 y subir datos en formato Athena"""

    def __init__(self):
        self._validate_credentials()
        self._setup_environment()
        self.client = self._create_client()
        self.bucket_name = settings.S3_BUCKET_NAME

    def _validate_credentials(self):
        """Valida que el archivo de credenciales exista"""
        if not os.path.exists(settings.AWS_CREDENTIALS_FILE):
            raise RuntimeError(
                f"Archivo de credenciales no encontrado en {settings.AWS_CREDENTIALS_FILE}"
            )

    def _setup_environment(self):
        """Configura las variables de entorno para boto3"""
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = settings.AWS_CREDENTIALS_FILE
        os.environ["AWS_CONFIG_FILE"] = settings.AWS_CONFIG_FILE
        os.environ["AWS_PROFILE"] = "default"

    def _create_client(self):
        """Crea el cliente de S3"""
        try:
            session = boto3.Session(profile_name='default')
            client = session.client('s3', region_name=settings.AWS_REGION)

            # Verificar conexión
            client.list_buckets()
            print(f"✓ Conexión exitosa a S3 en región {settings.AWS_REGION}", file=sys.stderr)

            return client
        except Exception as e:
            raise RuntimeError(f"Error al crear cliente S3: {str(e)}")

    def upload_jsonl(
            self,
            items: List[Dict[str, Any]],
            database_name: str,
            table_name: str
    ) -> str:
        """
        Sube datos en formato JSONL (JSON Lines) compatible con Athena
        Cada línea del archivo es un objeto JSON independiente

        Args:
            items: Lista de documentos/items
            database_name: Nombre de la base de datos (ej: 'dynamodb')
            table_name: Nombre de la tabla

        Returns:
            URL S3 del archivo subido
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Formato de ruta para particionamiento por fecha (compatible con Athena)
        date_partition = datetime.now().strftime("year=%Y/month=%m/day=%d")
        s3_key = f"{database_name}/{table_name}/{date_partition}/{table_name}_{timestamp}.json"

        try:
            # Convertir a formato JSONL (una línea JSON por documento)
            # Athena puede leer este formato directamente
            jsonl_content = "\n".join([
                json.dumps(item, ensure_ascii=False, default=str)
                for item in items
            ])

            # Crear buffer en memoria
            buffer = io.BytesIO(jsonl_content.encode('utf-8'))

            # Subir a S3
            self.client.upload_fileobj(
                buffer,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'Metadata': {
                        'source': 'dynamodb',
                        'table': table_name,
                        'record_count': str(len(items)),
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                }
            )

            print(f"✓ Archivo JSONL subido exitosamente: {s3_key}", file=sys.stderr)
            print(f"  Registros: {len(items)}", file=sys.stderr)

        except Exception as e:
            raise RuntimeError(f"Error subiendo archivo a S3: {str(e)}")

        s3_url = f"s3://{self.bucket_name}/{s3_key}"
        return s3_url

    def upload_json(
            self,
            items: List[Dict[str, Any]],
            database_name: str,
            table_name: str
    ) -> str:
        """
        Sube datos en formato JSON estándar (array de objetos)

        Args:
            items: Lista de documentos/items
            database_name: Nombre de la base de datos
            table_name: Nombre de la tabla

        Returns:
            URL S3 del archivo subido
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_partition = datetime.now().strftime("year=%Y/month=%m/day=%d")
        s3_key = f"{database_name}/{table_name}/{date_partition}/{table_name}_{timestamp}_array.json"

        try:
            # Convertir a JSON estándar (array)
            json_content = json.dumps(items, indent=2, ensure_ascii=False, default=str)

            buffer = io.BytesIO(json_content.encode('utf-8'))

            self.client.upload_fileobj(
                buffer,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'Metadata': {
                        'source': 'dynamodb',
                        'table': table_name,
                        'record_count': str(len(items)),
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                }
            )

            print(f"✓ Archivo JSON subido exitosamente: {s3_key}", file=sys.stderr)

        except Exception as e:
            raise RuntimeError(f"Error subiendo archivo JSON a S3: {str(e)}")

        s3_url = f"s3://{self.bucket_name}/{s3_key}"
        return s3_url

    def list_uploads(self, prefix: str = "") -> List[Dict[str, Any]]:
        """
        Lista archivos subidos en el bucket

        Args:
            prefix: Prefijo para filtrar (ej: 'dynamodb/locales/')

        Returns:
            Lista de objetos con información de archivos
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            objects = []
            for obj in response.get('Contents', []):
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    's3_url': f"s3://{self.bucket_name}/{obj['Key']}"
                })

            return objects

        except Exception as e:
            raise RuntimeError(f"Error listando objetos en S3: {str(e)}")