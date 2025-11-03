import boto3
import os
import sys
from typing import List, Dict, Any
from decimal import Decimal
from app.config import settings


class DynamoDBService:
    """Servicio para interactuar con DynamoDB"""

    def __init__(self):
        self._validate_credentials()
        self._setup_environment()
        self.client = self._create_client()
        self.resource = self._create_resource()

    def _validate_credentials(self):
        """Valida que el archivo de credenciales exista y sea legible"""
        if not os.path.exists(settings.AWS_CREDENTIALS_FILE):
            raise RuntimeError(
                f"Archivo de credenciales no encontrado en {settings.AWS_CREDENTIALS_FILE}. "
                f"Asegúrate de que el volumen esté montado correctamente."
            )

        if not os.access(settings.AWS_CREDENTIALS_FILE, os.R_OK):
            raise RuntimeError(
                f"No hay permisos de lectura en {settings.AWS_CREDENTIALS_FILE}"
            )

        # Verificar contenido
        try:
            with open(settings.AWS_CREDENTIALS_FILE, 'r') as f:
                content = f.read()
                if not content.strip():
                    raise RuntimeError("El archivo de credenciales está vacío")
                if '[default]' not in content:
                    raise RuntimeError("No se encontró el perfil [default] en credenciales")
                print("✓ Credenciales AWS encontradas y con formato válido", file=sys.stderr)
        except Exception as e:
            raise RuntimeError(f"Error leyendo credenciales: {str(e)}")

    def _setup_environment(self):
        """Configura las variables de entorno para boto3"""
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = settings.AWS_CREDENTIALS_FILE
        os.environ["AWS_CONFIG_FILE"] = settings.AWS_CONFIG_FILE
        os.environ["AWS_PROFILE"] = "default"

    def _create_client(self):
        """Crea el cliente de DynamoDB"""
        try:
            session = boto3.Session(profile_name='default')
            client = session.client('dynamodb', region_name=settings.AWS_REGION)

            # Verificar conexión
            client.list_tables()
            print(f"✓ Conexión exitosa a DynamoDB en región {settings.AWS_REGION}", file=sys.stderr)

            return client
        except Exception as e:
            raise RuntimeError(f"Error al crear cliente DynamoDB: {str(e)}")

    def _create_resource(self):
        """Crea el recurso de DynamoDB (interfaz de alto nivel)"""
        try:
            session = boto3.Session(profile_name='default')
            resource = session.resource('dynamodb', region_name=settings.AWS_REGION)
            return resource
        except Exception as e:
            raise RuntimeError(f"Error al crear recurso DynamoDB: {str(e)}")

    def scan_table(self, table_key: str) -> List[Dict[str, Any]]:
        """
        Escanea una tabla completa de DynamoDB

        Args:
            table_key: Clave de la tabla (ej: 'locales', 'usuarios')

        Returns:
            Lista de items de la tabla
        """
        table_name = settings.get_table_name(table_key)

        try:
            table = self.resource.Table(table_name)

            items = []
            response = table.scan()
            items.extend(response.get('Items', []))

            # Manejar paginación
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))

            # Convertir Decimals a float/int para JSON
            items = [self._convert_decimals(item) for item in items]

            print(f"✓ Escaneados {len(items)} items de {table_name}", file=sys.stderr)
            return items

        except Exception as e:
            raise RuntimeError(f"Error escaneando tabla {table_name}: {str(e)}")

    def _convert_decimals(self, obj: Any) -> Any:
        """
        Convierte objetos Decimal de DynamoDB a tipos nativos de Python
        para que sean serializables en JSON
        """
        if isinstance(obj, list):
            return [self._convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._convert_decimals(value) for key, value in obj.items()}
        elif isinstance(obj, Decimal):
            # Convertir a int si es entero, sino a float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        else:
            return obj

    def query_table(self, table_key: str, key_condition: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Realiza una query en una tabla DynamoDB

        Args:
            table_key: Clave de la tabla
            key_condition: Condición de la clave (ej: {'local_id': 'LOCAL-0001'})

        Returns:
            Lista de items que coinciden con la query
        """
        table_name = settings.get_table_name(table_key)

        try:
            table = self.resource.Table(table_name)

            # Construir expresión de clave
            from boto3.dynamodb.conditions import Key

            key_expr = None
            for attr, value in key_condition.items():
                if key_expr is None:
                    key_expr = Key(attr).eq(value)
                else:
                    key_expr = key_expr & Key(attr).eq(value)

            items = []
            response = table.query(KeyConditionExpression=key_expr)
            items.extend(response.get('Items', []))

            # Manejar paginación
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    KeyConditionExpression=key_expr,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response.get('Items', []))

            items = [self._convert_decimals(item) for item in items]

            return items

        except Exception as e:
            raise RuntimeError(f"Error en query de tabla {table_name}: {str(e)}")