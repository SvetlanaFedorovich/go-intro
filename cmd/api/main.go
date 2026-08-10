package main

import (
	"log/slog"
	"os"

	"github.com/AntonYurchenko/go-intro/internal/app/api"
)

func main() {
	if err := api.Run(); err != nil {
		slog.Error("api exited", "error", err)
		os.Exit(1)
	}
}
