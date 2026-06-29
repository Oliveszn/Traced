package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Oliveszn/Traced/server/handlers"
	"github.com/Oliveszn/Traced/server/store"
)

func main() {
	windowMinutes := getEnvInt("WINDOW_MINUTES", 30)
	window := time.Duration(windowMinutes) * time.Minute

	port := getEnvStr("PORT", "8080")

	log.Printf("starting traced server on :%s | window=%dm", port, windowMinutes)

	s := store.New(window)

	//  Background eviction
	// Runs every 30 seconds, removing spans that have aged out of the window
	// eviction must happen continuously, not only on the next ingest request
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.Evict()
			log.Printf("eviction complete | traces_in_store=%d", s.TraceCount())
		}
	}()

	mux := http.NewServeMux()
	h := handlers.New(s)
	h.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown on interrupt
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	_ = ctx

	log.Printf("listening on :%s", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

func getEnvStr(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("invalid value for %s: %v", key, err)
		}
		return n
	}
	return defaultVal
}
