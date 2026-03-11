package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

// Global bridge controller instance
var bridgeController *BridgeController

// Enums translated from Kotlin

type ExchangeType string

const (
	NSE ExchangeType = "NSE"
	BSE ExchangeType = "BSE"
)

type TransactionType string

const (
	BUY  TransactionType = "BUY"
	SELL TransactionType = "SELL"
)

type VarietyType string

const (
	REGULAR    VarietyType = "REGULAR"
	AMO        VarietyType = "AMO"
	GTT        VarietyType = "GTT"
	SQUARE_OFF VarietyType = "SQUARE_OFF"
)

type OrderType string

const (
	MARKET OrderType = "MARKET"
	LIMIT  OrderType = "LIMIT"
	SL     OrderType = "SL"
	SLM    OrderType = "SLM"
)

type ProductType string

const (
	CNC      ProductType = "CNC"
	INTRADAY ProductType = "INTRADAY"
)

type SegmentType string

const (
	EQUITY  SegmentType = "EQUITY"
	OPTIONS SegmentType = "OPTIONS"
)

type ValidityType string

const (
	DAY ValidityType = "DAY"
)

type Platform string

const (
	IOS     Platform = "IOS"
	ANDROID Platform = "ANDROID"
	WEB     Platform = "WEB"
	ALGO    Platform = "ALGO"
)

type OrderStatus string

const (
	RECEIVED OrderStatus = "RECEIVED"
	REJECTED OrderStatus = "REJECTED"
	COMPLETE OrderStatus = "COMPLETE"
)

type Link string

const (
	APP      Link = "APP"
	WEB_LINK Link = "WEB"
	SYSTEM   Link = "SYSTEM"
)

// DTOs and Models translated from Kotlin

// CreateMultipleOrderDTO corresponds to CreateMultipleOrderDto.kt
type CreateMultipleOrderDTO struct {
	Exchange         ExchangeType    `json:"exchange"`
	InstrumentID     string          `json:"instrumentId"`
	InstrumentSymbol string          `json:"instrumentSymbol"`
	BrokerSymbol     string          `json:"brokerSymbol"`
	TransactionType  TransactionType `json:"transactionType"`
	Variety          VarietyType     `json:"variety"`
	OrderType        OrderType       `json:"orderType"`
	Product          ProductType     `json:"product"`
	SegmentType      SegmentType     `json:"segmentType"`
	Quantity         int             `json:"quantity"`
	Price            *float64        `json:"price,omitempty"`
	TriggerPrice     *float64        `json:"triggerPrice,omitempty"`
	PlatformID       Platform        `json:"platformId,omitempty"`
}

// OrderDTO corresponds to the internal OrderDto object
type OrderDTO struct {
	CreateMultipleOrderDTO
	UserID string
}

// Order corresponds to the main Order domain object
type Order struct {
	OrderDTO
	OrderID int64       `json:"orderId"`
	Status  OrderStatus `json:"status"`
	Reason  string      `json:"reason"`
	Link    Link        `json:"link"`
}

// QbOrderRequest is the structure for placing an order via the WebSocket bridge.
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
	OrderID string      `json:"orderId"`
	Status  OrderStatus `json:"status"`
	Reason  string      `json:"reason,omitempty"`
	UserID  string      `json:"userId"`
}

// Simplified representation of external data
type Instrument struct {
	InstrumentID string
	BrokerSymbol string
	SegmentType  SegmentType
	LotSize      int
}

type Account struct {
	UserID  string
	Balance float64
}

type MarginResponse struct {
	TotalRequired float64
}

// --- Mock Services and Helpers ---

// In-memory store for user orders with a mutex for thread-safe access
var (
	userOrders       = make(map[string][]Order)
	orderIDToUserMap = make(map[int64]string)
	ordersMutex      = &sync.Mutex{}
)

// Mock for fetching instrument data from Redis
func getInstrumentData(instrumentID string) (*Instrument, error) {
	log.Printf("Fetching instrument data for ID: %s", instrumentID)
	// In a real app, this would query Redis/DB
	return &Instrument{
		InstrumentID: instrumentID,
		BrokerSymbol: "RELIANCE-EQ",
		SegmentType:  EQUITY,
		LotSize:      1,
	}, nil
}

// Mock for fetching margin requirements
func fetchMargin(order Order) (*MarginResponse, error) {
	log.Printf("Fetching margin for order: %+v", order)
	price := 0.0
	if order.Price != nil {
		price = *order.Price
	}
	required := price * float64(order.Quantity)
	return &MarginResponse{TotalRequired: required}, nil
}

// Mock for fetching account balance
func fetchAccountData(userID string) (*Account, error) {
	log.Printf("Fetching account data for user: %s", userID)
	return &Account{UserID: userID, Balance: 100000.0}, nil // Mock balance
}

// Mock for fetching charges
func fetchCharges(order Order) float64 {
	log.Printf("Fetching charges for order: %+v", order)
	return 20.0 // Mock charges
}

// Mock for generating a unique order ID
func generateOrderID() int64 {
	return time.Now().UnixNano()
}

// Corresponds to logic in CreateSingleOrderUseCase
func createSingleOrders(orderDTOs []OrderDTO) []Order {
	var processedOrders []Order

	for _, dto := range orderDTOs {
		instrument, err := getInstrumentData(dto.InstrumentID)
		if err != nil {
			log.Printf("Failed to get instrument data: %v", err)
			continue
		}

		if dto.Quantity%instrument.LotSize != 0 {
			log.Printf("Quantity is not a multiple of lot size for %s", dto.InstrumentSymbol)
			continue
		}

		order := Order{OrderDTO: dto}

		margin, err := fetchMargin(order)
		if err != nil {
			log.Printf("Failed to fetch margin: %v", err)
			continue
		}

		charges := fetchCharges(order)
		account, err := fetchAccountData(dto.UserID)
		if err != nil {
			log.Printf("Failed to fetch account data: %v", err)
			continue
		}

		// Check 1: Sufficient Balance
		if account.Balance < margin.TotalRequired+charges {
			order.Status = REJECTED
			order.Reason = fmt.Sprintf("Insufficient funds. Required: %.2f, Available: %.2f", margin.TotalRequired+charges, account.Balance)
			processedOrders = append(processedOrders, order)
			continue
		}

		// Check 2: Platform ID Validation
		allowedPlatforms := map[Platform]bool{
			WEB:     true,
			IOS:     true,
			ANDROID: true,
			ALGO:    true,
		}
		if !allowedPlatforms[dto.PlatformID] {
			order.Status = REJECTED
			order.Reason = fmt.Sprintf("Restricted PlatformId %s", dto.PlatformID)
			processedOrders = append(processedOrders, order)
			continue
		}
		if account.Balance >= margin.TotalRequired+charges {
			order.OrderID = generateOrderID()
			order.Status = RECEIVED
			order.Reason = "Order placed successfully"

			if order.PlatformID == WEB {
				order.Link = WEB_LINK
			} else if order.PlatformID == IOS || order.PlatformID == ANDROID {
				order.Link = APP
			} else {
				order.Link = SYSTEM
			}

			// --- Send order to bridge ---
			price := 0.0
			if order.Price != nil {
				price = *order.Price
			}
			qbOrder := QbOrderRequest{
				OrderID:   fmt.Sprintf("%d", order.OrderID),
				Price:     price,
				Qty:       order.Quantity,
				Symbol:    order.InstrumentSymbol,
				Side:      string(order.TransactionType),
				Account:   "mock_account_id", // Placeholder for actual account ID
				OrderType: string(order.OrderType),
				UserID:    order.UserID,
			}
			orderJSON, err := json.Marshal(qbOrder)
			if err != nil {
				log.Printf("Failed to marshal order to JSON: %v", err)
			} else {
				bridgeController.Outgoing <- orderJSON
				log.Printf("Sent order %d to bridge", order.OrderID)
			}

		} else {
			log.Printf("Insufficient balance. Available: %.2f, Required: %.2f", account.Balance, margin.TotalRequired+charges)
			order.OrderID = generateOrderID()
			order.Status = REJECTED
			order.Reason = fmt.Sprintf("Insufficient Funds. Required: %.2f", margin.TotalRequired+charges)
		}
		processedOrders = append(processedOrders, order)
	}

	return processedOrders
}

// Corresponds to logic in OrderController
func placeOrder(dtos []CreateMultipleOrderDTO, userID string, platform Platform) ([]Order, error) {
	log.Printf("Received place order request for user %s with %d orders", userID, len(dtos))

	if len(dtos) == 0 {
		return nil, errors.New("order data cannot be empty")
	}

	var orderDTOs []OrderDTO
	for _, dto := range dtos {
		if dto.PlatformID == "" {
			dto.PlatformID = platform
		}
		orderDTOs = append(orderDTOs, OrderDTO{CreateMultipleOrderDTO: dto, UserID: userID})
	}

	createdOrders := createSingleOrders(orderDTOs)

	for _, order := range createdOrders {
		if order.Status == REJECTED {
			log.Printf("Order %d was rejected: %s", order.OrderID, order.Reason)
			return createdOrders, fmt.Errorf("order rejected: %s", order.Reason)
		}
	}

	return createdOrders, nil
}

// --- HTTP Server ---

func placeOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := r.Header.Get("userId")
	if userID == "" {
		http.Error(w, "Header 'userId' is required", http.StatusBadRequest)
		return
	}
	platform := Platform(r.Header.Get("platformId"))

	var orderRequest []CreateMultipleOrderDTO
	if err := json.NewDecoder(r.Body).Decode(&orderRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	finalOrders, err := placeOrder(orderRequest, userID, platform)

	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusCreated)
	}

	if jsonErr := json.NewEncoder(w).Encode(finalOrders); jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
		return
	}

	ordersMutex.Lock()
	defer ordersMutex.Unlock()
	for _, order := range finalOrders {
		if order.Status != REJECTED {
			userOrders[userID] = append(userOrders[userID], order)
			orderIDToUserMap[order.OrderID] = userID
		}
	}
}

func getOrdersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := r.URL.Query().Get("userId")
	if userID == "" {
		http.Error(w, "userId query parameter is required", http.StatusBadRequest)
		return
	}

	ordersMutex.Lock()
	orders, exists := userOrders[userID]
	ordersMutex.Unlock()

	if !exists {
		orders = []Order{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if jsonErr := json.NewEncoder(w).Encode(orders); jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
	}
}

// BridgeController manages the WebSocket connection to the mock exchange.
type BridgeController struct {
	conn     *websocket.Conn
	wsURL    string
	Outgoing chan []byte
	Incoming chan []byte
	stop     chan struct{}
}

// NewBridgeController creates and initializes a new BridgeController.
func NewBridgeController(wsURL string) *BridgeController {
	return &BridgeController{
		wsURL:    wsURL,
		Outgoing: make(chan []byte, 100), // Buffered channel
		Incoming: make(chan []byte, 100), // Buffered channel
		stop:     make(chan struct{}),
	}
}

// Start begins the connection and message handling loops.
func (bc *BridgeController) Start() {
	log.Println("Starting order bridge controller...")
	go bc.manageConnection()
}

// Stop gracefully shuts down the controller.
func (bc *BridgeController) Stop() {
	log.Println("Stopping order bridge controller...")
	close(bc.stop)
}

// manageConnection handles the connection lifecycle, including reconnection.
func (bc *BridgeController) manageConnection() {
	for {
		select {
		case <-bc.stop:
			log.Println("Exiting connection manager.")
			return
		default:
			log.Printf("Attempting to connect to WebSocket at %s...", bc.wsURL)
			ws, err := websocket.Dial(bc.wsURL, "", "http://localhost/")
			if err != nil {
				log.Printf("Failed to connect: %v. Retrying in 5 seconds...", err)
				time.Sleep(5 * time.Second)
				continue
			}

			bc.conn = ws
			log.Println("WebSocket connection established.")

			// Connection successful, start read/write/heartbeat loops
			done := make(chan struct{})
			go bc.writeLoop(done)
			go bc.readLoop(done)
			go bc.heartbeatLoop(done)

			<-done          // Wait until a loop exits (due to error)
			bc.conn.Close() // Ensure connection is closed before retrying
			log.Println("Connection lost. Will attempt to reconnect.")
		}
	}
}

// readLoop continuously reads messages from the WebSocket.
func (bc *BridgeController) readLoop(done chan struct{}) {
	defer func() {
		log.Println("Read loop terminated.")
		close(done) // Signal that this connection is done
	}()

	for {
		var msg string
		if err := websocket.Message.Receive(bc.conn, &msg); err != nil {
			log.Printf("Error in read loop: %v", err)
			return
		}
		// Non-blocking send to Incoming channel
		select {
		case bc.Incoming <- []byte(msg):
		default:
			log.Println("Incoming message channel is full. Message dropped.")
		}
	}
}

// writeLoop sends messages from the Outgoing channel to the WebSocket.
func (bc *BridgeController) writeLoop(done chan struct{}) {
	defer log.Println("Write loop terminated.")
	for {
		select {
		case message := <-bc.Outgoing:
			if err := websocket.Message.Send(bc.conn, string(message)); err != nil {
				log.Printf("Error in write loop: %v", err)
				return // Error will be caught by manageConnection to reconnect
			} else {
				log.Printf("Sent message: %s", string(message))
			}
		case <-done: // If read loop fails, stop writing
			return
		}
	}
}

// heartbeatLoop sends a periodic heartbeat to keep the connection alive.
func (bc *BridgeController) heartbeatLoop(done chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		ticker.Stop()
		log.Println("Heartbeat loop terminated.")
	}()

	for {
		select {
		case <-ticker.C:
			heartbeatMsg := `{"reqType":"heartbeat"}`
			if err := websocket.Message.Send(bc.conn, heartbeatMsg); err != nil {
				log.Printf("Error sending heartbeat: %v", err)
				return // Exit to trigger reconnection
			}
		case <-done:
			return
		}
	}
}

func main() {
	// Initialize and start the WebSocket bridge controller
	bridgeController = NewBridgeController("ws://localhost:8080/ws")
	bridgeController.Start()

	// Start a goroutine to listen for incoming messages from the bridge
	go func() {
		for msg := range bridgeController.Incoming {
			log.Printf("Received message from bridge: %s", string(msg))

			var response ExchangeResponse
			if err := json.Unmarshal(msg, &response); err != nil {
				log.Printf("Error unmarshalling exchange response: %v", err)
				continue
			}

			orderID, err := strconv.ParseInt(response.OrderID, 10, 64)
			if err != nil {
				log.Printf("Could not parse order ID from response: %v", err)
				continue
			}

			ordersMutex.Lock()
			var orderFound bool
			userID, ok := orderIDToUserMap[orderID]
			if ok {
				if userOrdersForUser, ok := userOrders[userID]; ok {
					for i, order := range userOrdersForUser {
						if order.OrderID == orderID {
							userOrdersForUser[i].Status = response.Status
							userOrdersForUser[i].Reason = response.Reason
							log.Printf("Updated order %d for user %s to status %s", orderID, userID, response.Status)
							orderFound = true
							break
						}
					}
				}
			}
			ordersMutex.Unlock()

			if !orderFound {
				log.Printf("Could not find order with ID %d to update status", orderID)
			}
		}
	}()

	log.Println("Starting order validation service on :8082")

	http.HandleFunc("/api/order", placeOrderHandler)
	http.HandleFunc("/api/orders", getOrdersHandler)

	log.Fatal(http.ListenAndServe(":8082", nil))
}
