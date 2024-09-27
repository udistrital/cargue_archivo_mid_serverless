package main

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"strconv"

	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

var base64Data = "MSwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMjQ1LDIKMiwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMTkwLDQKMywyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMTkyLDQKNCwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMTkzLDMKNSwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMTk1LDIKNiwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMTk5LDQKNywyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMjAwLDQKOCwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMjMyLDMKOSwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMjM4LDQKMTAsMjAyNC0wOS0zMCwyMDI0LTA5LTEzLGZhbHNlLDIzOSwyCjExLDIwMjQtMDktMzAsMjAyNC0wOS0xMyxmYWxzZSwyMjcsMgoxMiwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMjMwLDQKMTMsMjAyNC0wOS0zMCwyMDI0LTA5LTEzLGZhbHNlLDI0Myw0CjE0LDIwMjQtMDktMzAsMjAyNC0wOS0xMyxmYWxzZSwyNDQsMwoxNSwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMTg2LDIKMTYsMjAyNC0wOS0zMCwyMDI0LTA5LTEzLGZhbHNlLDE4Nyw0CjE3LDIwMjQtMDktMzAsMjAyNC0wOS0xMyxmYWxzZSwyMDIsNAoxOCwyMDI0LTA5LTMwLDIwMjQtMDktMTMsZmFsc2UsMjAzLDMKMTksMjAyNC0wOS0zMCwyMDI0LTA5LTEzLGZhbHNlLDIwNSw0CjIwLDIwMjQtMDktMzAsMjAyNC0wOS0xMyxmYWxzZSwyMDYsMg=="
var endpointURL = "https://3ff1-170-78-41-251.ngrok-free.app/v1/periodos-rol-usuarios/"

// type Response struct {
// 	Nombre             string `json:"Nombre"`
// 	SistemaInformacion struct {
// 		Id int `json:"Id"`
// 	} `json:"SistemaInformacionId"`
// }

type Response struct {
	FechaFin    string `json:"FechaFin"`
	FechaInicio string `json:"FechaInicio"`
	Finalizado  bool   `json:"Finalizado"`
	RolId       struct {
		Id                  int `json:"Id"`
	} `json:"RolId"`
	UsuarioId struct {
		Id int `json:"Id"`
	} `json:"UsuarioId"`
}

func decodificarBase64(data string) ([]byte, error) {
	datos, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("Error al decodificar base64: %v", err)
	}
	return datos, nil
}

func leerCSV(data []byte) ([][]string, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))
	registros, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("Error al leer el CSV: %v", err)
	}
	return registros, nil
}

func enviarRegistro(jsonData []byte) error {
	resp, err := http.Post(endpointURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("Error al enviar los datos al endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("Error: el servidor devolvió un estado %d", resp.StatusCode)
	}
	return nil
}

func procesarCSV(registros [][]string) (string, error) {
	if len(registros) == 0 || len(registros[0]) < 6 {
		return "", fmt.Errorf("CSV no valido")
	}

	for _, registro := range registros {

		id := registro[0]
		fechaFin:= registro[1]
		fechaInicio:= registro[2]
		finalizadoStr:= registro[3]
		rolIdInt:= registro[4]
		usuarioIdInt:= registro[5]

		finalizado, err := strconv.ParseBool(finalizadoStr)

		// Crear la estructura JSON
		response := Response{
			FechaFin:    fechaFin,
			FechaInicio: fechaInicio,
			Finalizado:  finalizado,
		}

		var usuarioId int
		fmt.Sscanf(usuarioIdInt, "%d", &usuarioId)
		response.UsuarioId.Id = usuarioId

		var rolID int
		fmt.Sscanf(rolIdInt, "%d", &rolID)
		response.RolId.Id = rolID

		// Convertir la estructura a JSON
		jsonData, err := json.Marshal(response)
		if err != nil {
			return "", fmt.Errorf("Error al crear el registro %s: %v", id, err)
		}

		// Enviar el JSON al endpoint
		err = enviarRegistro(jsonData)
		if err != nil {
			log.Printf("Error al enviar el registro %s: %v", id, err)
			continue
		}

		fmt.Printf("Registro con id %s enviado: %s\n", id, string(jsonData))
	}

	return "Datos enviados correctamente", nil
	
}

// Función que será llamada por Lambda
func handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	// Decodificar los datos base64
	data, err := decodificarBase64(base64Data)
	if err != nil {
		log.Printf("Error: %v", err)
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       err.Error(),
		}, nil
	}

	// Leer el CSV
	registros, err := leerCSV(data)
	if err != nil {
		log.Printf("Error: %v", err)
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       err.Error(),
		}, nil
	}

	// Procesar los registros
	resultado, err := procesarCSV(registros)
	if err != nil {
		log.Printf("Error: %v", err)
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       err.Error(),
		}, nil
	}

	// Respuesta
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       resultado,
	}, nil
}

func main() {
	lambda.Start(handler)
}
