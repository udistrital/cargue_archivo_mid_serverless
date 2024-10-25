import json
import os


def get_headers():
    return {
        "Access-Control-Allow-Origin": "http://localhost:4200",
        "Access-Control-Allow-Methods": " POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
    }

def parse_body(event) -> tuple:
    """
    Deserialización de parámetros de entrada que tengan body.
    """
    try:
        return json.loads(event["body"]), None
    except Exception as ex:
        print(f"Error in parse_body - lambda 'registrar_datos_archivo'. Details: {str(ex)}")
        return None, ex

def format_response(result, message: str, status_code: int, success: bool) -> dict:
    """
    Crea la estructura de respuesta con los campos:
        statusCode: código HTTP
        headers: encabezados para peticiones locales - CORS
        body: JSON con la siguiente estructura
            Success: Campo booleano que indica si fue exitosa (true) o no la petición
            Status: código HTTP
            Message: mensaje descriptivo del resultado de la petición
            Data: No es agregado en caso de un error en las peticiones, contiene un
            diccionario con el resultado de la petición
    """
    body = {
        "Success": success,
        "Status": status_code,
        "Message": message
    }
    if success:
        body["Data"] = result

    response = {
        "statusCode": status_code,
        "body": json.dumps(body)
    }

    is_local = os.environ.get('IS_LOCAL', None)
    if is_local:
        headers = get_headers()
        response['headers'] = headers
    return response


def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        if http_method == 'POST':
            body, error = parse_body(event)
            if error is None:
                # Implementa tu código para registrar los datos del archivo
                result = {}
                message = "Documento procesado correctamente"
                return format_response(
                    result,
                    message,
                    200,
                    True
                )
            else:
                return format_response(
                    None,
                    f"Error el payload no sigue el formato JSON esperado",
                    400,
                    False
                )
        elif http_method == 'OPTIONS':
            return format_response(
                None,
                "OK",
                200,
                True
            )
        else:
            return format_response(
                None,
                "Metodo no permitido",
                405,
                False
            )
    except Exception as e:
        print(f"Error in lambda_handler - lambda 'registrar_datos_archivo'. Details: {str(e)}")
        return format_response(
            None,
            "Error registrando los datos del archivo",
            500,
            False
        )
