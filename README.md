# Carga masiva de datos - Serverless MID

Lambda que permite leer un archivo en base64, procesar los datos y registrarlos en un API CRUD.

## Especificaciones Técnicas

### Tecnologías Implementadas y Versiones
* [Python 3.12](https://docs.python.org/3.12/)
* [uv](https://docs.astral.sh/uv/getting-started/installation/)
* [AWS SAM](https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/using-sam-cli.html)
* [AWS SAM CLI](https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/install-sam-cli.html)
* Opcional (Requerido para ejecutar el servicio API en local, simula el API Gateway) [Docker](https://docs.docker.com/engine/install/ubuntu/)

### Dependencias
Las dependencias se declaran en `pyproject.toml` y se gestionan con uv (ya no se usa `requirements.txt`).
```shell
uv sync                  # instala las dependencias en el entorno local
uv add <paquete>         # agrega una dependencia de la Lambda
uv add --dev <paquete>   # agrega una dependencia solo de desarrollo
```
**Nota:** la función se construye con `BuildMethod: python-uv`, que en AWS SAM está en versión preliminar, por lo que `sam build` requiere `--beta-features`. Ver [Building Python Lambda functions with uv](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/building-python-uv.html).

### Ejecución del Proyecto en Local
```shell
sam build --beta-features
sam local start-api --env-vars env.example.json
```
**Nota:**
* Para más detalle de las formas de ejecutarlo localmente vea [Uso sam local](https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/using-sam-cli-local.html)
* Puede usar el script `run_local.sh` para correr los comandos indicados anteriormente con bash. 

### Ejecución Pruebas

Linter y formato
```shell
uv run ruff check .
uv run ruff format .
```

Pruebas unitarias
```shell
# En Proceso
```

### Despliegue
```shell
sam build --beta-features
sam deploy --guided
```
**Nota:** 
* Para mayor información para realizar el despliegue vea [Uso sam deploy](https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/using-sam-cli-deploy.html).

## Estado CI


## Licencia
