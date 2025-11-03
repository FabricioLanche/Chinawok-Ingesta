# ChinaWok Data Ingestion API

API REST desarrollada con FastAPI para extraer datos de tablas DynamoDB y depositarlos en S3 en formato compatible con Amazon Athena.

## Descripción

Sistema de ingesta de datos que permite extraer información de las tablas DynamoDB de ChinaWok (Locales, Usuarios, Productos, Empleados, Combos, Pedidos, Ofertas y Reseñas) y almacenarlas en formato JSONL (JSON Lines) en S3, optimizado para consultas con Amazon Athena.

## Estructura del Proyecto

```
.
├── app/
│   ├── __init__.py
│   ├── config.py              # Configuración centralizada
│   └── services/
│       ├── __init__.py
│       ├── dynamodb_service.py # Servicio de extracción DynamoDB
│       └── s3_service.py       # Servicio de carga a S3
├── main.py                     # Aplicación FastAPI principal
├── requirements.txt            # Dependencias Python
├── Dockerfile                  # Imagen Docker
├── docker-compose.yml          # Orquestación de contenedores
├── .env                        # Variables de entorno (no versionado)
├── .gitignore
├── .dockerignore
└── README.md
```

## Requisitos Previos

- Docker y Docker Compose instalados
- Credenciales AWS configuradas en `~/.aws/credentials`
- Acceso a DynamoDB y S3 en AWS

## Configuración

1. Crear archivo `.env` en la raíz del proyecto:

```bash
# S3 Bucket
S3_BUCKET_NAME=tu-bucket-name

# DynamoDB Tables
TABLE_LOCALES=ChinaWok-Locales
TABLE_USUARIOS=ChinaWok-Usuarios
TABLE_PRODUCTOS=ChinaWok-Productos
TABLE_EMPLEADOS=ChinaWok-Empleados
TABLE_COMBOS=ChinaWok-Combos
TABLE_PEDIDOS=ChinaWok-Pedidos
TABLE_OFERTAS=ChinaWok-Ofertas
TABLE_RESENAS=ChinaWok-Resenas
```

2. Configurar credenciales AWS en `~/.aws/credentials`:

```ini
[default]
aws_access_key_id=YOUR_ACCESS_KEY
aws_secret_access_key=YOUR_SECRET_KEY
aws_session_token=YOUR_SESSION_TOKEN
```

3. Configurar región AWS en `~/.aws/config` (opcional, por defecto usa `us-east-1`):

```ini
[default]
region=us-east-1
```

## Uso

### Iniciar el servicio

```bash
docker-compose up --build
```

La API estará disponible en `http://localhost:8000`

### Endpoints Disponibles

#### Health Check
```bash
GET /
GET /health
```

#### Ingestar tabla específica
```bash
POST /ingest/{table_name}
```

Ejemplo:
```bash
curl -X POST http://localhost:8000/ingest/locales
```

#### Ingestar todas las tablas
```bash
POST /ingest/all
```

Ejemplo:
```bash
curl -X POST http://localhost:8000/ingest/all
```

### Documentación Interactiva

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Formato de Salida

Los datos se almacenan en S3 con la siguiente estructura simplificada:

```
s3://chinawok-data/
├── locales/
│   └── locales_20251102_143000.json
├── usuarios/
│   └── usuarios_20251102_143001.json
├── productos/
│   └── productos_20251102_143002.json
├── empleados/
│   └── empleados_20251102_143003.json
├── combos/
│   └── combos_20251102_143004.json
├── pedidos/
│   └── pedidos_20251102_143005.json
├── ofertas/
│   └── ofertas_20251102_143006.json
└── resenas/
    └── resenas_20251102_143007.json
```

Formato: **JSONL (JSON Lines)** - cada línea es un objeto JSON independiente, compatible con Athena.

## Desarrollo Local (sin Docker)

```bash
# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar servidor
uvicorn main:app --reload
```

## Notas Técnicas

- Las credenciales AWS se montan como volumen de solo lectura
- Los datos DynamoDB Decimal se convierten automáticamente a tipos nativos Python
- El particionamiento por fecha facilita las consultas en Athena
- Los metadatos incluyen información de extracción en cada archivo S3