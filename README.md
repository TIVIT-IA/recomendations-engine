# ¿Qué es recommendations-engine?
Es un servicio que:
    * Trae datos de trabajadores desde diferentes APIs.
    * Limpia y ordena toda la información para que todos los registros tengan el mismo formato.
    * Convierte ese texto en números especiales (vectores) que sirven para hacer:
        - búsquedas inteligentes,
        - recomendaciones,
        - encontrar perfiles parecidos,
        - y análisis más avanzados.

En pocas palabras:
agarra datos desordenados → los ordena → y los convierte en algo que una IA pueda buscar e interpretar.

# INFRAESTRUCTURA
TEI - INGESTOR - REDIS

TEI: Es el traductor 
Convierte texto normal (nombre, experiencia, etc.) en un vector (una lista de números) que las búsquedas inteligentes pueden entender. "El ingestor le manda texto y TEI responde con los números."

INGESTOR: El ingestor es el cerebro del sistema. 
- Llama a las APIs externas para traer datos.
- Revisa qué datos cambiaron y cuáles no.
- Arma un texto unificado del trabajador.
- Pide a TEI que convierta ese texto en un vector.
- Guarda todo eso en la base de datos.
"Es como un robot que revisa, ordena y actualiza la información todo el tiempo."


REDIS: Redis es como un cuaderno rápido donde se guardan cosas en memoria. Sirve para:
- acelerar procesos,
- guardar datos temporales,
- y ayudar al ingestor a organizarse mejor.
"No guarda información “importante”, solo cosas rápidas para que todo vaya más fluido."

## USOS
1. Búsquedas inteligentes de personal
Puedes buscar trabajadores como si hablaras con una persona: “Muéstrame alguien que tenga experiencia en redes Cisco y viva en Arequipa”.

2. Recomendación de perfiles para proyectos

3. Sugerencia de cursos o certificaciones
Si el motor sabe lo que un trabajador sabe (por experiencia + certificaciones),
- puede recomendar:
- Cursos que le faltan
- Certificaciones relevantes
- Rutas de aprendizaje

“Dime qué capacitación sería ideal para este trabajador.”

4. Detección de brechas de habilidades
Comparar trabajadores, áreas o equipos respecto a ciertas habilidades:



## Levantar en desarrollo 

1. LEVANTAR TEI (Text Embeddings Inference)

cmd: docker run -d --name tei -p 8090:80 -e MODEL_ID=intfloat/e5-small ghcr.io/huggingface/text-embeddings-inference:cpu-latest

El endpoint principal para embeddings es POST /embed con payload {"inputs": ["texto1","texto2"]}

NOTA: Si TEI no funciona
*  cmd: wf.msc
*  agrega una regla de entrada para:
    * Puerto: 8090
    * Protocolo: TCP
    * Acción: permitir

NOTA: Prueba si TEI funciona
cmd: curl -X POST "http://localhost:8090/embed" -H "Content-Type: application/json" -d "{\"inputs\": [\"hola mundo\"]}"


2. LEVANTAR REDIS

cmd: docker run -d --name redis -p 6379:6379 redis:7

    probar que funcione  (cmd):
    * cmd: docker ps
    * entra al contenedor : docker exec -it redis redis-cli
        Dentro de CLI de redis
            * SET test "hola"
            * GET test



3. LEVANTAR INGESTOR

* crear entorno virtual : python -m venv .venv
* levantar entorno virtual : .\.venv\Scripts\Activate.ps1
* instalar requirements : pip install -r requirements.txt


4. EJECUTAR SERVICIO: 

* python -m ingestor.main




