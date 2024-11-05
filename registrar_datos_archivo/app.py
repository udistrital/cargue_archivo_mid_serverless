import json
import os
import base64
import io
import pandas as pd
import requests


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

def decode_base64(base64_data: str) -> io.BytesIO:
    """
    Decodifica el archivo base64.
    """
    try:
        decoded_file = base64.b64decode(base64_data)
        return io.BytesIO(decoded_file)
    except Exception as ex:
        print(f"Error al decodificar base64. : {str(ex)}")
        return None

def read_file(file: io.BytesIO) -> pd.DataFrame:
    """
    Lee el archivo decodificado en memoria.
    """
    try:
        df = pd.read_excel(file)
        df = df.dropna(how='all')
        print(df)
        return df
    except Exception as ex:
        print(f"Error al leer el archivo. : {str(ex)}")
        return None
    
def validate_data(df: pd.DataFrame, structure: dict) -> bool:
    """
    Verifica que las columnas en el archivo coincidan con las de la estructura.
    """    
    expected_columns = set([config.get('file_name_column') for config in structure.values()])
    file_columns = set(df.columns)
    
    missing_columns = expected_columns - file_columns

    if missing_columns:
        print(f"Error: Faltan las siguientes columnas en el archivo: {missing_columns}")
        return False

    return True
    
def build_url (service: str, endpoint: str) -> str:
    """
    Construye la URL para la petición.
    """
    return f"{service.rstrip('/')}/{endpoint.lstrip('/')}"

def parse_value(value, parse_type):
    """
    Parsea el valor de la celda.
    """
    if parse_type == "int":
        return int(value)
    elif parse_type == "booleano":
        return bool(value)
    elif parse_type == "date":
        return value.strftime('%Y-%m-%d')
    else:
        return value

def prepare_payload(row, structure):
    """
    Crea el payload para la petición.
    """
    payload = {}
    try:
        for key, config in structure.items():
            file_name_column = config.get('file_name_column')

            value = row[file_name_column]

            if pd.isna(value) or value is None:
                if not config.get("required"):
                    continue
                else:                    
                    raise ValueError(f"El campo '{key}' es requerido y está vacío en la fila.")

            parse_type = config.get("parse")
            value = parse_value(value, parse_type)
            

            keys = key.split(".")
            temp = payload
            for k in keys[:-1]:
                if k not in temp:
                    temp[k] = {}
                temp = temp[k]
            temp[keys[-1]] = value


        print(payload)
        return payload
    
    except Exception as ex:
        print(f"Error al preparar el payload. : {str(ex)}")


def send_request(payload, url: str) -> bool:
    """
    Envia cada fila de la tabla al endpoint.
    """
    try:
        response = requests.post(url, json=payload)
        return response.status_code in [200, 201]

    except Exception as ex:
        print(f"Error al enviar la petición. : {str(ex)}")
        return False

def process_file(df: pd.DataFrame, structure: dict, url: str):
    """
    Procesa cada fila de la tabla.
    """
    try:
        for index, row in df.iterrows():

            payload = prepare_payload(row, structure)
            success = send_request(payload, url)
            if success:
                print(f"Fila {index} registrada correctamente.")
            else:
                print(f"Error fila {index} no se pudo registrar.")

    except Exception as ex:
        print(f"Error al procesar el archivo. : {str(ex)}")

def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        if http_method == 'POST':
            body, error = parse_body(event)
            if error is None:
                # Implementa tu código para registrar los datos del archivo
                base64_file = body.get("base64data")
                if not base64_file:
                    return format_response(
                        None,
                        "Archivo base64 no encontrado",
                        400,  
                        False
                    )
                
                decoded_file = decode_base64(base64_file)
                df = read_file(decoded_file)
                if df is None:
                   return format_response(
                       None,
                       "Error al leer el archivo.",
                       500,
                       False
                   )
                
                service = body.get("service")
                endpoint = body.get("endpoint")
                structure = body.get("structure")

                if not service or not endpoint or not structure:
                    return format_response(
                        None,
                        "Faltan parametros en la estructura",
                        400,
                        False
                    )
                
                if not validate_data(df, structure):
                    return format_response(
                        None,
                        "Las columnas del archivo no coinciden con la estructura esperada.",
                        400,
                        False
                    )
                
                url = build_url(service, endpoint)
                print(url)
                process_file(df, structure, url)

                return format_response(
                    None,
                    "Documento procesado correctamente",
                    200,
                    True
                )

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
