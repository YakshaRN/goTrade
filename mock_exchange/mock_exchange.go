package main

import (
	"log"
	"net/http"

	"encoding/json"
	"golang.org/x/net/websocket"
	"math/rand"
	"time"
)

// GenericRequest is used to determine the type of the incoming message.
type GenericRequest struct {
	ReqType string `json:"reqType"`
}

// QbOrderRequest is a simplified structure for incoming order requests.
type QbOrderRequest struct {
	OrderID   string  `json:"orderId"`
	Price     float64 `json:"price"`
	Qty       int     `json:"qty"`
	Symbol    string  `json:"symbol"`
	Side      string  `json:"side"`
	Account   string  `json:"account"`
	OrderType string  `json:"orderType"`
	UserID    string  `json:"userId"`
}

// ExchangeResponse represents a response from the mock exchange.
type ExchangeResponse struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	UserID  string `json:"userId"`
}

// handleWebSocket handles incoming WebSocket connections.
func handleWebSocket(ws *websocket.Conn) {
	log.Println("New client connected:", ws.RemoteAddr())
	defer func() {
		log.Println("Client disconnected:", ws.RemoteAddr())
		ws.Close()
	}()

	// Loop to read messages from the client
	for {
		var msg string
		if err := websocket.Message.Receive(ws, &msg); err != nil {
			// Check for a clean disconnect.
			if err.Error() == "EOF" {
				log.Println("Client closed connection gracefully.")
			} else {
				log.Println("Error receiving message:", err)
			}
			break
		}

		log.Printf("Received message from %s: %s", ws.RemoteAddr(), msg)

		// First, check if it's a heartbeat.
		var genericReq GenericRequest
		if err := json.Unmarshal([]byte(msg), &genericReq); err == nil && genericReq.ReqType == "heartbeat" {
			log.Println("Received heartbeat.")
			continue // Skip to the next message
		}

		// If not a heartbeat, assume it's an order request.
		var orderReq QbOrderRequest
		if err := json.Unmarshal([]byte(msg), &orderReq); err != nil {
			log.Printf("Error unmarshalling order request: %v. Message: %s", err, msg)
			continue // Skip to the next message
		}

		// 2. Simulate order processing and create a response.
		log.Printf("Processing order %s for user %s", orderReq.OrderID, orderReq.UserID)
		response := ExchangeResponse{
			OrderID: orderReq.OrderID,
			UserID:  orderReq.UserID,
		}

		// Randomly accept or reject the order to simulate a real exchange.
		if rand.Intn(2) == 0 { // 50% chance of rejection
			response.Status = "REJECTED"
			response.Reason = "Insufficient margin"
		} else {
			response.Status = "COMPLETE"
		}

		// 3. Marshal the response to JSON and send it back.
		responseJSON, err := json.Marshal(response)
		if err != nil {
			log.Printf("Error marshalling response: %v", err)
			continue
		}

		if err := websocket.Message.Send(ws, responseJSON); err != nil {
			log.Println("Error sending message:", err)
			break
		}
	}
}

func main() {
	// Seed the random number generator.
	rand.Seed(time.Now().UnixNano())

	log.Println("Starting mock exchange WebSocket server on :8080")

	// The WebSocket handler will be at ws://localhost:8080/ws
	http.Handle("/ws", websocket.Handler(handleWebSocket))

	// Start the server
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
