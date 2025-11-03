from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
from datetime import datetime
import sys

from app.services.dynamodb_service import DynamoDBService
from app.services.s3_service import S3Service
from app.config import settings

app = FastAPI(
    title="ChinaWok Data Ingestion API",
    description="API para extraer datos de DynamoDB y depositarlos en S3 formato Athena",
    version="1.0.0"
)

# Inicializar servicios
dynamodb_service = DynamoDBService()
s3_service = S3Service()


@app.get("/")
async def root():
    """Endpoint de health check"""
    return {
        "service": "ChinaWok Data Ingestion API",
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }


@app.post("/ingest/all")
async def ingest_all_tables():
    """
    Extrae datos de todas las tablas DynamoDB y las sube a S3
    """
    tables = [
        "locales", "usuarios", "productos", "empleados",
        "combos", "pedidos", "ofertas", "resenas"
    ]

    results = []
    errors = []
    total_records = 0

    for table_name in tables:
        try:
            print(f"Procesando tabla: {table_name}", file=sys.stderr)

            # Extraer datos directamente del servicio
            items = dynamodb_service.scan_table(table_name)

            if items:
                # Subir a S3
                s3_url = s3_service.upload_jsonl(items, "dynamodb", table_name)
                results.append({
                    "table": table_name,
                    "status": "success",
                    "records": len(items),
                    "s3_location": s3_url
                })
                total_records += len(items)
                print(f"✓ {table_name}: {len(items)} registros subidos", file=sys.stderr)
            else:
                results.append({
                    "table": table_name,
                    "status": "empty",
                    "records": 0,
                    "s3_location": None
                })
                print(f"⚠ {table_name}: tabla vacía", file=sys.stderr)

        except Exception as e:
            error_msg = f"{table_name}: {str(e)}"
            errors.append({
                "table": table_name,
                "error": str(e)
            })
            results.append({
                "table": table_name,
                "status": "error",
                "records": 0,
                "s3_location": None
            })
            print(f"✗ Error en {table_name}: {str(e)}", file=sys.stderr)

    return {
        "status": "completed" if not errors else "completed_with_errors",
        "total_records": total_records,
        "tables_processed": len(tables),
        "results": results,
        "errors": errors if errors else None,
        "timestamp": datetime.now().isoformat()
    }


@app.post("/ingest/{table_name}")
async def ingest_table(table_name: str):
    """
    Extrae datos de una tabla DynamoDB específica y los sube a S3

    Args:
        table_name: Nombre de la tabla (locales, usuarios, productos, etc.)
    """
    try:
        # Validar nombre de tabla
        valid_tables = [
            "locales", "usuarios", "productos", "empleados",
            "combos", "pedidos", "ofertas", "resenas"
        ]

        if table_name.lower() not in valid_tables:
            raise HTTPException(
                status_code=400,
                detail=f"Tabla no válida. Tablas disponibles: {', '.join(valid_tables)}"
            )

        print(f"Iniciando ingesta de tabla: {table_name}", file=sys.stderr)

        # Extraer datos de DynamoDB
        items = dynamodb_service.scan_table(table_name)

        if not items:
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"Tabla {table_name} está vacía",
                    "records_extracted": 0
                }
            )

        print(f"✓ Extraídos {len(items)} registros de {table_name}", file=sys.stderr)

        # Subir a S3 en formato Athena (JSONL - una línea por documento)
        s3_url = s3_service.upload_jsonl(items, "dynamodb", table_name)

        return {
            "status": "success",
            "table": table_name,
            "records_extracted": len(items),
            "s3_location": s3_url,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        print(f"✗ Error en ingesta de {table_name}: {str(e)}", file=sys.stderr)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Verifica la salud de las conexiones AWS"""
    health_status = {
        "dynamodb": "unknown",
        "s3": "unknown",
        "timestamp": datetime.now().isoformat()
    }

    # Verificar DynamoDB
    try:
        dynamodb_service.client.list_tables()
        health_status["dynamodb"] = "healthy"
    except Exception as e:
        health_status["dynamodb"] = f"unhealthy: {str(e)}"

    # Verificar S3
    try:
        s3_service.client.list_buckets()
        health_status["s3"] = "healthy"
    except Exception as e:
        health_status["s3"] = f"unhealthy: {str(e)}"

    all_healthy = all(
        status == "healthy"
        for key, status in health_status.items()
        if key != "timestamp"
    )

    return JSONResponse(
        status_code=200 if all_healthy else 503,
        content=health_status
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )