package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

const batchSize = 1000

type Email struct {
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func processTar(filePath string) {
	// 1. Descomprimir el archivo
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error al abrir el archivo:", err)
		return
	}
	defer file.Close()

	tarReader := tar.NewReader(file)
	batch := make([]Email, 0)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error al leer tar:", err)
			return
		}

		if header.Typeflag == tar.TypeReg && header.Name != "_sent_mail" {
			// 2. Procesar los correos electrónicos
			buf := new(bytes.Buffer)
			io.Copy(buf, tarReader)
			emailContent := buf.String()

			email := processEmail(emailContent)
			if email != nil {
				batch = append(batch, *email)

				if len(batch) >= batchSize {
					sendBatch(batch)
					batch = make([]Email, 0)
				}
			}
		}
	}
	if len(batch) > 0 {
		sendBatch(batch)
	}
}

func processEmail(content string) *Email {
	// Puedes adaptar este método para procesar adecuadamente los correos electrónicos.
	// De momento, simplemente devuelve el contenido como cuerpo del correo.
	return &Email{
		Subject: "TODO: Procesar el asunto",
		Body:    content,
	}
}

func sendBatch(batch []Email) {
	ndjson := ""
	for _, email := range batch {
		jsonData, _ := json.Marshal(email)
		ndjson += string(jsonData) + "\n"
	}

	req, err := http.NewRequest("POST", "http://localhost:4080/api/_bulk", bytes.NewBuffer([]byte(ndjson)))
	if err != nil {
		fmt.Println("Error al crear la solicitud:", err)
		return
	}

	req.SetBasicAuth("admin", "maiden")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error al enviar el lote:", err)
		return
	}

	defer resp.Body.Close()
	fmt.Println("Lote enviado, estado de respuesta:", resp.Status)
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: ./indexer <path-to-enron-tar>")
		return
	}
	tarPath := os.Args[1]
	processTar(tarPath)
}
