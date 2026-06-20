package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

type MovieEvent struct {
	ID        int64  `json:"id"`
	MovieID   int64  `json:"movie_id"`
	Title     string `json:"title"`
	Action    string `json:"action"`
	UserID    int64  `json:"user_id"`
	Timestamp string `json:"timestamp"`
}

type UserEvent struct {
	ID        int64  `json:"id"`
	UserID    int64  `json:"user_id"`
	Username  string `json:"username"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	ID         int64   `json:"id"`
	PaymentID  int64   `json:"payment_id"`
	UserID     int64   `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type"`
}

var (
	movieWriter   *kafka.Writer
	userWriter    *kafka.Writer
	paymentWriter *kafka.Writer
)

// Генератор уникальных ID на основе атомарного счётчика
var idCounter int64

func generateID() int64 {
	return atomic.AddInt64(&idCounter, 1)
}

func initKafka() {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:9092"
	}

	movieWriter = &kafka.Writer{
		Addr:  kafka.TCP(kafkaBrokers),
		Topic: "movie-events",
	}

	userWriter = &kafka.Writer{
		Addr:  kafka.TCP(kafkaBrokers),
		Topic: "user-events",
	}

	paymentWriter = &kafka.Writer{
		Addr:  kafka.TCP(kafkaBrokers),
		Topic: "payment-events",
	}
}

func closeKafka() {
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := movieWriter.Close(); err != nil {
		log.Printf("Error closing movie writer: %v", err)
	}
	if err := userWriter.Close(); err != nil {
		log.Printf("Error closing user writer: %v", err)
	}
	if err := paymentWriter.Close(); err != nil {
		log.Printf("Error closing payment writer: %v", err)
	}
}

func publishToKafka(ctx context.Context, writer *kafka.Writer, event interface{}) error {
	msg, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return writer.WriteMessages(ctx, kafka.Message{
		Value: msg,
	})
}

func subscribeToMovieEvents(ctx context.Context) {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokers},
		GroupID: "events-service",
		Topic:   "movie-events",
	})

	go func() {
		defer reader.Close()
		for {
			select {
			case <-ctx.Done():
				log.Println("Movie events subscriber shutting down")
				return
			default:
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Error reading movie event: %v", err)
					continue
				}
				log.Printf("[MOVIE EVENT] Received: %s", string(m.Value))
			}
		}
	}()
}

func subscribeToUserEvents(ctx context.Context) {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokers},
		GroupID: "events-service",
		Topic:   "user-events",
	})

	go func() {
		defer reader.Close()
		for {
			select {
			case <-ctx.Done():
				log.Println("User events subscriber shutting down")
				return
			default:
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Error reading user event: %v", err)
					continue
				}
				log.Printf("[USER EVENT] Received: %s", string(m.Value))
			}
		}
	}()
}

func subscribeToPaymentEvents(ctx context.Context) {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokers},
		GroupID: "events-service",
		Topic:   "payment-events",
	})

	go func() {
		defer reader.Close()
		for {
			select {
			case <-ctx.Done():
				log.Println("Payment events subscriber shutting down")
				return
			default:
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Error reading payment event: %v", err)
					continue
				}
				log.Printf("[PAYMENT EVENT] Received: %s", string(m.Value))
			}
		}
	}()
}

func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    true,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func createMovieEvent(c *gin.Context) {
	var req struct {
		MovieID int64  `json:"movie_id" binding:"required"`
		Title   string `json:"title" binding:"required"`
		Action  string `json:"action" binding:"required"`
		UserID  int64  `json:"user_id" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	event := MovieEvent{
		ID:        generateID(),
		MovieID:   req.MovieID,
		Title:     req.Title,
		Action:    req.Action,
		UserID:    req.UserID,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if err := publishToKafka(c.Request.Context(), movieWriter, event); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish event"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":     event.ID,
		"status": "success",
	})
}

func createUserEvent(c *gin.Context) {
	var req struct {
		UserID   int64  `json:"user_id" binding:"required"`
		Username string `json:"username" binding:"required"`
		Action   string `json:"action" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	event := UserEvent{
		ID:        generateID(),
		UserID:    req.UserID,
		Username:  req.Username,
		Action:    req.Action,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if err := publishToKafka(c.Request.Context(), userWriter, event); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish event"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":     event.ID,
		"status": "success",
	})
}

func createPaymentEvent(c *gin.Context) {
	var req struct {
		PaymentID  int64   `json:"payment_id" binding:"required"`
		UserID     int64   `json:"user_id" binding:"required"`
		Amount     float64 `json:"amount" binding:"required"`
		Status     string  `json:"status" binding:"required"`
		MethodType string  `json:"method_type" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	event := PaymentEvent{
		ID:         generateID(),
		PaymentID:  req.PaymentID,
		UserID:     req.UserID,
		Amount:     req.Amount,
		Status:     req.Status,
		Timestamp:  time.Now().Format(time.RFC3339),
		MethodType: req.MethodType,
	}

	if err := publishToKafka(c.Request.Context(), paymentWriter, event); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish event"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":     event.ID,
		"status": "success",
	})
}

func main() {
	initKafka()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subscribeToMovieEvents(ctx)
	subscribeToUserEvents(ctx)
	subscribeToPaymentEvents(ctx)

	router := gin.Default()

	router.GET("/api/events/health", healthCheck)
	router.POST("/api/events/movie", createMovieEvent)
	router.POST("/api/events/user", createUserEvent)
	router.POST("/api/events/payment", createPaymentEvent)

	go func() {
		<-ctx.Done()
		log.Println("Shutting down events service...")
		closeKafka()
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	log.Printf("Starting events service on port %s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
