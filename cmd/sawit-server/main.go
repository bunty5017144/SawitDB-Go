package main

import (
	"github.com/WowoEngine/SawitDB-Go/internal/server"
)

func main() {
	port := 7878
	// Parsing flags or env logic here

	srv := server.NewSawitServer(server.Config{
		Port:           port,
		Host:           "0.0.0.0",
		DataDir:        "data",
		MaxConnections: 100,
		LogLevel:       "info",
	})
	srv.Start()
}
