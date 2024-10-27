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


def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        if http_method == 'POST':
            body, error = parse_body(event)
            if error is None:
                # Implementa tu código para registrar los datos del archivo
                archivo_base64 = body.get("base64data")
                if not archivo_base64:
                    return format_response(
                        None,
                        "Archivo base64 no encontrado",
                        400,
                        False
                    )
                #Decodificar base64
                archivo_decodificado = base64.b64decode(archivo_base64)
                archivo_en_memoria = io.BytesIO(archivo_decodificado)

                #leer el archivo
                df = pd.read_excel(archivo_en_memoria)
                df = df.dropna(how='all')
                
                print(df)

                url = "https://fde7-170-78-41-251.ngrok-free.app/v1/periodos-rol-usuarios/"

                for index, row in df.iterrows():
                    if row.isnull().any():
                        print(f"Fila {index} contiene celdas vacías.")
                        continue

                    fecha_inicio = row['FechaInicio'].strftime('%Y-%m-%d')
                    fecha_fin = row['FechaFin'].strftime('%Y-%m-%d')

                    payload ={
                        "FechaInicio": fecha_inicio,
                        "FechaFin": fecha_fin,
                        "Finalizado": row["Finalizado"],
                        "RolId": {"Id": int(row["Rol"])},
                        "UsuarioId": {"Id": int(row["Usuario"])}
                    }

                    response = requests.post(url, json=payload)

                    if response.status_code in [200, 201]:
                        print(f"Fila {index} registrada correctamente.")
                    else:
                        print(f"Error fila {index} no se pudo registrar, estado: {response.status_code}.")    
                
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
