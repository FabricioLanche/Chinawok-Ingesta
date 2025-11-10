# Chinawok Ingesta

Proyecto serverless para ingesta de datos desde DynamoDB hacia S3.

## Arquitectura

- **Lambda 1 (ingestTable)**: Ingesta una tabla específica de DynamoDB
- **Lambda 2 (ingestAllTables)**: Ingesta todas las tablas configuradas

## Requisitos

- Node.js 18+
- Python 3.11
- AWS CLI configurado
- Serverless Framework

## Configuración

1. Copiar `.env.example` a `.env` y configurar las variables
2. Instalar dependencias:
```bash
npm install
```

## Deploy

```bash
# Deploy a AWS
serverless deploy

# Deploy a un stage específico
serverless deploy --stage prod --region us-east-1
```

## Uso

### Ingestar tabla específica
```bash
curl -X POST https://your-api-gateway-url/ingest/locales
```

### Ingestar todas las tablas
```bash
curl -X POST https://your-api-gateway-url/ingest/all
```

## Tablas disponibles

- locales
- usuarios
- productos
- empleados
- combos
- pedidos
- ofertas
- resenas

## Logs

```bash
# Ver logs de función específica
serverless logs -f ingestTable --tail

# Ver logs de todas las tablas
serverless logs -f ingestAllTables --tail
```

## Remover deployment

```bash
serverless remove
```