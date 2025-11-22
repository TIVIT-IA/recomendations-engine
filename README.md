# recomendations-engine
Servicio que consume y sincroniza datos de trabajadores desde APIs externas para procesarlos y transformarlos en vectores, alimentando una base de datos vectorial actualizada. Permite habilitar búsquedas semánticas, recomendaciones y análisis avanzados con información consistente y siempre al día.


## Levantar en desarrollo (Docker Compose)

1. Copia `.env.example` a `.env` y ajusta variables si es necesario.
2. `docker-compose up --build`
3. Espera que Postgres e Infra estén listos.
4. Ajusta `fetch_source_batch()` en `ingestor/src/ingest_worker.py` para conectar con tu fuente de datos.

## Notas
- El SQL usa `vector(1024)` (modelo intfloat/multilingual-e5-base). Ajusta si usas otro modelo.
- Reemplaza `ingestor/src/fetch_source_batch` con lógica real para ingestión desde APIs, S3 o archivos.



#### TEI (Text Embeddings Inference) - instrucciones rápidas ####

Levantar TEI en CPU:

docker run -d --name tei -p 8080:80 ghcr.io/huggingface/text-embeddings-inference:latest --model-id intfloat/e5-small

Levantar TEI en GPU (si tienes GPU y driver configurado):

docker run -d --name tei -p 8090:80 -e MODEL_ID=intfloat/e5-small ghcr.io/huggingface/text-embeddings-inference:cpu-latest

El endpoint principal para embeddings es POST /embed con payload {"inputs": ["texto1","texto2"]}

#### Si TEI no funciona: 

1. cmd: wf.msc
2. agrega una regla de entrada para:
    * Puerto: 8090
    * Protocolo: TCP
    * Acción: permitir

## Prueba si TEI funciona 

cmd: curl -X POST "http://localhost:8090/embed" -H "Content-Type: application/json" -d "{\"inputs\": [\"hola mundo\"]}"


#### REDIS ####
Redis hace que tu sistema sea más rápido, más ordenado y más eficiente, guardando cosas en memoria y manejando tareas.

Ejecutar: 
cmd: docker run -d --name redis -p 6379:6379 redis:7

    probar que funcione  (cmd):
    * cmd: docker ps
    * entra al contenedor : docker exec -it redis redis-cli
        Dentro de CLI de redis
            * SET test "hola"
            * GET test

