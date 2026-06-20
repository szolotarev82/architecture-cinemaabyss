package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

func loadConfig() *Config {
	return &Config{
		Port:                   getEnv("PORT", "8000"),
		MonolithURL:            getEnv("MONOLITH_URL", "http://monolith:8080"),
		MoviesServiceURL:       getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"),
		GradualMigration:       getBoolEnv("GRADUAL_MIGRATION", false),
		MoviesMigrationPercent: getIntEnv("MOVIES_MIGRATION_PERCENT", 50),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getBoolEnv(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value == "true" {
		return true
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if val, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return val
	}
	return defaultValue
}

type ProxyHandler struct {
	config *Config
}

func newProxyHandler(config *Config) *ProxyHandler {
	return &ProxyHandler{config: config}
}

func (h *ProxyHandler) shouldUseNewService() bool {
	return rand.Intn(100) < h.config.MoviesMigrationPercent
}

func (h *ProxyHandler) determineTarget(r *http.Request) string {
	path := r.URL.Path
	if strings.HasPrefix(path, "/api/movies") {
		if h.config.GradualMigration && h.shouldUseNewService() {
			return h.config.MoviesServiceURL
		}
		return h.config.MonolithURL
	}
	return h.config.MonolithURL
}

func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	targetURL := h.determineTarget(r)

	fullURL := targetURL + r.URL.Path
	if r.URL.RawQuery != "" {
		fullURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequest(r.Method, fullURL, r.Body)
	if err != nil {
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}

	proxyReq.Header = r.Header.Clone()

	client := &http.Client{}
	resp, err := client.Do(proxyReq)
	if err != nil {
		http.Error(w, "Failed to forward request", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for name, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		http.Error(w, "Failed to copy response", http.StatusInternalServerError)
		return
	}
}

// Обработчик хелсчека
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{"status": "OK"}
	json.NewEncoder(w).Encode(response)
}

func main() {
	cfg := loadConfig()
	proxyHandler := newProxyHandler(cfg)

	// Создаём мультиплексор для маршрутизации
	mux := http.NewServeMux()
	mux.HandleFunc("/api/proxy/health", healthHandler)
	mux.Handle("/", proxyHandler)

	port := ":" + cfg.Port
	log.Printf("Starting proxy service on port %s", port)
	log.Printf("Monolith URL: %s", cfg.MonolithURL)
	log.Printf("Movies Service URL: %s", cfg.MoviesServiceURL)
	log.Printf("Gradual Migration: %t", cfg.GradualMigration)
	log.Printf("Movies Migration Percent: %d%%", cfg.MoviesMigrationPercent)
	log.Fatal(http.ListenAndServe(port, mux))
}
