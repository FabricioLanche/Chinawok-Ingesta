import os
from typing import Dict
from dotenv import load_dotenv
import configparser

load_dotenv()


class Settings:
    """Configuración centralizada de la aplicación"""

    # AWS Configuration
    AWS_CREDENTIALS_FILE: str = "/root/.aws/credentials"
    AWS_CONFIG_FILE: str = "/root/.aws/config"
    AWS_REGION: str = "us-east-1"  # Default

    def __init__(self):
        # Intentar leer la región del archivo config de AWS
        self.AWS_REGION = self._get_aws_region()

    def __init__(self):
        # Intentar leer la región del archivo config de AWS
        self.AWS_REGION = self._get_aws_region()

    def _get_aws_region(self) -> str:
        """
        Lee la región de AWS desde el archivo de configuración
        Si no existe, retorna 'us-east-1' por defecto
        """
        try:
            if os.path.exists(self.AWS_CONFIG_FILE):
                config = configparser.ConfigParser()
                config.read(self.AWS_CONFIG_FILE)

                # Intentar leer del perfil default
                if 'default' in config and 'region' in config['default']:
                    region = config['default']['region']
                    print(f"✓ Región AWS leída del config: {region}")
                    return region

            # Si no se encuentra, usar default
            print(f"⚠ No se encontró región en {self.AWS_CONFIG_FILE}, usando us-east-1")
            return "us-east-1"

        except Exception as e:
            print(f"⚠ Error leyendo región AWS: {e}, usando us-east-1")
            return "us-east-1"

    # S3 Configuration
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "")

    # DynamoDB Table Names
    DYNAMODB_TABLES: Dict[str, str] = {
        "locales": os.getenv("TABLE_LOCALES", "ChinaWok-Locales"),
        "usuarios": os.getenv("TABLE_USUARIOS", "ChinaWok-Usuarios"),
        "productos": os.getenv("TABLE_PRODUCTOS", "ChinaWok-Productos"),
        "empleados": os.getenv("TABLE_EMPLEADOS", "ChinaWok-Empleados"),
        "combos": os.getenv("TABLE_COMBOS", "ChinaWok-Combos"),
        "pedidos": os.getenv("TABLE_PEDIDOS", "ChinaWok-Pedidos"),
        "ofertas": os.getenv("TABLE_OFERTAS", "ChinaWok-Ofertas"),
        "resenas": os.getenv("TABLE_RESENAS", "ChinaWok-Resenas"),
    }

    def validate(self):
        """Valida que las variables de entorno requeridas estén configuradas"""
        errors = []

        if not self.S3_BUCKET_NAME:
            errors.append("S3_BUCKET_NAME no está configurado en .env")

        if errors:
            raise ValueError(f"Errores de configuración: {', '.join(errors)}")

    def get_table_name(self, table_key: str) -> str:
        """Obtiene el nombre real de la tabla DynamoDB"""
        table_name = self.DYNAMODB_TABLES.get(table_key.lower())
        if not table_name:
            raise ValueError(f"Tabla '{table_key}' no configurada")
        return table_name


# Instancia global de configuración
settings = Settings()

# Validar configuración al importar
try:
    settings.validate()
except ValueError as e:
    print(f"⚠ Advertencia de configuración: {e}")
    print("El servicio puede no funcionar correctamente")