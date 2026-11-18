package main

import (
	"log"

	"go-backend-service-blueprint/config"
)

// @title           Go Backend Service Blueprint API
// @version         1.0
// @description     Reference HTTP API of the service blueprint.
// @BasePath        /
func main() {
	cfg := config.InitHTTPAppConfig()
	app := NewHTTPApp(cfg)
	defer app.Close()

	if err := app.Run(); err != nil {
		log.Fatalf("http app exited with error: %v", err)
	}
}
