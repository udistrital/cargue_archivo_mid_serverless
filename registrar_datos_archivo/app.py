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
        return None, f"Error en el cuerpo de la solicitud: {str(ex)}"

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

def decode_base64(base64_data: str) -> tuple:
    """
    Decodifica el archivo base64.
    """
    try:
        decoded_file = base64.b64decode(base64_data)
        return io.BytesIO(decoded_file), None
    except Exception as ex:
        return None, f"Error al decodificar archivo base64: {str(ex)}"

def read_file(file: io.BytesIO) ->tuple:
    """
    Lee el archivo decodificado en memoria.
    """
    try:
        df = pd.read_excel(file)
        df = df.dropna(how='all')
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        print("DataFrame:\n", df.head())
        return df, None
    except Exception as ex:
        return None, f"Error al leer el archivo: {str(ex)}"
    
def validate_data(df: pd.DataFrame, structure: dict) -> tuple:
    """
    Verifica que las columnas en el archivo coincidan con las de la estructura.    """    

    expected_columns = set()
    for config in structure.values():
        file_name_column = config.get('file_name_column')
        if file_name_column:
            expected_columns.add(file_name_column)
        
        column_group = config.get('column_group')
        if column_group:
            expected_columns.update(column_group)
    
    file_columns = set(df.columns)

    missing_columns = expected_columns - file_columns

    if missing_columns:
        return False, f"Faltan las siguientes columnas en el archivo: {missing_columns}"
    return True, None
    
def build_url (service: str, endpoint: str) -> tuple:
    """
    Construye la URL para la petición.
    """
    if not service or not endpoint:
        return None, "Falta 'service' o 'endpoint' en la solicitud."
    try:
        url = f"{service.rstrip('/')}/{endpoint.lstrip('/')}"
        return url, None
    except Exception as ex:
        return None, f"Error al construir la URL: {str(ex)}"

def map_value(value, mapping):
    """
    Mapea el valor de la estructura si es necesario.
    """
    try:
        if isinstance(value, str):
            mapped_value = mapping.get(value.lower())
        else:
            mapped_value = value
        if mapped_value is None:
            raise ValueError(f"Valor '{value}' no encontrado.")
        return mapped_value, None
    except Exception as ex:
        return None, f"Error en el mapeo del valor '{value}': {str(ex)}" 
    

def parse_value(value, parse_type, mapping = None):
    """
    Parsea el valor de la celda.
    """
    try:
        if parse_type == "int":
            return int(value), None
        elif parse_type == "booleano":
            return bool(value), None
        elif parse_type == "date":
            return value.strftime('%Y-%m-%d'), None
        else:
            return value, None
    except Exception as ex:
        return None, f"Error en conversión del valor '{value}': {str(ex)}"
    
def add_complement(payload: dict, complement: dict) -> tuple:
    """
    Agrega datos adicionales al payload si es necesario.
    """
    try:
        if complement:
            payload.update(complement)
        return payload, None
    except Exception as ex:
        return None, f"Error al agregar complemento al payload: {str(ex)}"
    
def get_columns(row, column_names) :
    """
     Extrae los nombres de las columnas que tienen datos en una fila.
    """
    return [col for col in column_names if pd.notna(row.get(col))and row.get(col) != ""]
    

def prepare_payload(row, structure) -> tuple:
    """
    Crea el payload para la petición.
    """
    payload = {}
    try:
        for key, config in structure.items():
            if "column_group" in config:
                column_names = config.get("column_group", [])
                non_empty_columns = get_columns(row, column_names)
                payload[key] = non_empty_columns
                continue

            file_name_column = config.get('file_name_column')
            value = row.get(file_name_column)

            if pd.isna(value) or value is None:
                if not config.get("required"):
                    continue
                else:                    
                    raise ValueError(f"El campo '{key}' es requerido y está vacío en la fila.")
                

            mapping = config.get("mapping")
            if mapping:
                value, error = map_value(value, mapping)
                if error:
                    return None, error
            else:
                parse_type = config.get("parse")
                value, error = parse_value(value, parse_type, mapping)
                if error:
                    return None, error            

            keys = key.split(".")
            temp = payload
            for k in keys[:-1]:
                if k not in temp:
                    temp[k] = {}
                temp = temp[k]
            temp[keys[-1]] = value
        
        return payload, None
    
    except Exception as ex:
        return None, f"Error al preparar el payload: {str(ex)}"

def send_request(payload, url: str) -> tuple:
    """
    Envia cada fila de la tabla al endpoint.
    """
    try:
        response = requests.post(url, json=payload)
        if response.status_code not in [200, 201]:
            return False, f"Error al enviar la petición:{response.status_code} - {response.text}"
        return True, None
    except Exception as ex:
        return False, f"Error al enviar la petición: {str(ex)}"

def process_file(df: pd.DataFrame, structure: dict, url: str, complement: dict) -> tuple:
    """
    Procesa cada fila de la tabla.
    """
    correct_indices = []
    error_details = []

    for index, row in df.iterrows():
        payload, error = prepare_payload(row, structure)
        if error:
            error_details.append({"Idx": index, "Error": error})
            continue

        payload, error = add_complement(payload, complement)
        if error:
            error_details.append({"Idx": index, "Error": error})
            continue

        print("Payload:\n", payload)

        success, send_error = send_request(payload, url)
        if success:
            correct_indices.append(index)
        else:
            error_details.append({"Idx": index, "Error": send_error})

    return {"Correctos": correct_indices, "Erróneos": error_details}, None

def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        if http_method == 'POST':
            body, error = parse_body(event)
            if error:
                return format_response(
                    None,    
                    error, 
                    400, 
                    False
                )
            
            base64_file = body.get("base64data")
            if not base64_file:
                return format_response(
                    None,
                    "Archivo base64 no encontrado",
                    400,  
                    False
                )
                
            decoded_file, decode_error = decode_base64(base64_file)
            if decode_error:
                return format_response(
                    None,
                    decode_error,
                    400,
                    False 
                )
            df, read_error = read_file(decoded_file)
            if read_error:
                return format_response(
                    None,
                    read_error,
                    500,
                    False
                )
                
            service = body.get("service")
            endpoint = body.get("endpoint")
            structure = body.get("structure")
            complement = body.get("complement", {})

            if not service or not endpoint or not structure:
                return format_response(
                    None,
                    "Faltan parametros en la estructura",
                    400,
                    False
                )
            
            url, url_error = build_url(service, endpoint)
            print(url)
            if url_error:
                return format_response(
                    None,
                    url_error,
                    400,
                    False
                )
            
            valid, validate_error = validate_data(df, structure)
            if not valid:
                return format_response(
                    None,
                    validate_error,
                    400,
                    False
                )
            
            result, process_error = process_file(df, structure, url, complement)
            if process_error:
                return format_response(
                    None,
                    process_error,
                    500,
                    False
                )
            
            message = "Documento procesado correctamente" if not result ["Erróneos"] else "Documento procesado con algunos registros por revisar"
            return format_response(
                result,
                message,
                201 if not result ["Erróneos"] else 206,
                True
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
