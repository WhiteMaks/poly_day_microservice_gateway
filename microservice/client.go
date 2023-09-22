package microservice

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"log"
	"time"
)

type Clients map[*Client]bool

type Client struct {
	connection *websocket.Conn
	ms         *Microservice
	events     chan Event
}

var (
	heartBeatMaxTime  = 10 * time.Second
	heartBeatInterval = 9 * time.Second
)

func newClient(connection *websocket.Conn, ms *Microservice) *Client {
	return &Client{
		connection: connection,
		ms:         ms,
		events:     make(chan Event),
	}
}

func (client *Client) closeConnection() {
	client.connection.Close()
}

func (client *Client) updateReadDeadline() error {
	err := client.connection.SetReadDeadline(time.Now().Add(heartBeatMaxTime))
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) setupHeartBeat() error {
	err := client.updateReadDeadline()
	if err != nil {
		return err
	}

	client.connection.SetPongHandler(client.heartBeatHandler)

	return nil
}

func (client *Client) readMessages() {
	defer func() {
		client.ms.DeleteClient(client)
	}()

	err := client.setupHeartBeat()
	if err != nil {
		return
	}

	for {
		messageType, payload, err := client.connection.ReadMessage()

		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNoStatusReceived) {
				log.Printf("error reading message : %v", err)
			}
			break
		}

		log.Printf("message type: %x, payload: %v from client", messageType, string(payload))

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

	heartBeating := time.NewTicker(heartBeatInterval)

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
		case <-heartBeating.C:
			err := client.connection.WriteMessage(websocket.PingMessage, []byte(``))
			if err != nil {
				return
			}
		}
	}
}

func (client *Client) AddEvent(event Event) {
	client.events <- event
}

func (client *Client) heartBeatHandler(message string) error {
	return client.updateReadDeadline()
}
