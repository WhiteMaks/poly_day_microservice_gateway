package microservice

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"log"
)

type Clients map[*Client]bool

type Client struct {
	wsId       string
	userId     int64
	connection *websocket.Conn
	ms         *Microservice
	events     chan Event
}

func newClient(connection *websocket.Conn, ms *Microservice) *Client {
	return &Client{
		userId:     -1,
		wsId:       uuid.New().String(),
		connection: connection,
		ms:         ms,
		events:     make(chan Event),
	}
}

func (client *Client) closeConnection() {
	client.connection.Close()
}

func (client *Client) readMessages() {
	defer func() {
		client.ms.DeleteClient(client)
	}()

	for {
		_, payload, err := client.connection.ReadMessage()

		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNoStatusReceived) {
				log.Printf("error reading message : %v", err)
			}
			break
		}

		var message Message
		err = json.Unmarshal(payload, &message)
		if err != nil {
			log.Printf("error marshaling message: %v", err)
			break
		}

		err = client.ms.routeEvent(message, client)
		if err != nil {
			log.Println(err)
			break
		}
	}
}

func (client *Client) writeMessages() {
	defer func() {
		client.ms.DeleteClient(client)
	}()

	for {
		select {
		case message, success := <-client.events:
			if !success {
				err := client.connection.WriteMessage(websocket.CloseMessage, nil)
				if err != nil {
					log.Printf("connection closed: %v", err)
				}
				return
			}

			data, err := json.Marshal(message)
			if err != nil {
				log.Println(err)
				return
			}

			err = client.connection.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				log.Printf("failed to send message to client: %v", err)
				return
			}
		}
	}
}

func (client *Client) AddEvent(event Event) {
	client.events <- event
}
