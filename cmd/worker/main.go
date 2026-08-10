package main

import (
	"log/slog"
	"os"

	"github.com/AntonYurchenko/go-intro/internal/app/worker"
)

func main() {
	if err := worker.Run(); err != nil {
		slog.Error("worker exited", "error", err)
		os.Exit(1)
	}
}
