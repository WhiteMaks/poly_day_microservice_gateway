package microservice

import (
	"encoding/json"
	"github.com/WhiteMaks/poly_day_bus_schema/schema"
	"github.com/WhiteMaks/poly_day_bus_schema/wbus"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"sync"
)

type Microservice struct {
	bus     *wbus.Rabbit
	clients Clients
	sync.RWMutex
}

type Event struct {
	Microservice string `json:"microservice"` //from microservice
	//Type         string          `json:"type"`
	Method  string          `json:"method"`
	Version string          `json:"version"`
	Status  int64           `json:"status"`
	Body    json.RawMessage `json:"body"`
}

type Message struct {
	Microservice string `json:"microservice"` //to microservice
	//Type         string          `json:"type"`
	Method  string          `json:"method"`
	Version string          `json:"version"`
	Body    json.RawMessage `json:"body"`
}

func NewMs(bus *wbus.Rabbit) *Microservice {
	ms := &Microservice{}

	ms.bus = bus
	ms.clients = make(Clients)

	return ms
}

func (ms *Microservice) Start(address string, path string) error {
	http.HandleFunc(path, func(writer http.ResponseWriter, request *http.Request) {
		ms.upgradeProtocol(writer, request)
	})

	err := http.ListenAndServe(address, nil)
	if err != nil {
		return err
	}

	return nil
}

func (ms *Microservice) upgradeProtocol(writer http.ResponseWriter, request *http.Request) {
	wsUpgrade := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     checkOrigin,
	}

	connection, err := wsUpgrade.Upgrade(writer, request, nil)
	if err != nil {
		log.Println(err)
		return
	}

	wsClient := newClient(connection, ms)
	ms.saveClient(wsClient)

	go wsClient.readMessages()
	go wsClient.writeMessages()
}

func (ms *Microservice) saveClient(client *Client) {
	ms.Lock()

	ms.clients[client] = true

	ms.Unlock()
}

func (ms *Microservice) DeleteClient(wsClient *Client) {
	ms.Lock()

	_, found := ms.clients[wsClient]
	if found {
		wsClient.closeConnection()
		delete(ms.clients, wsClient)
		log.Printf("client disconected")
	}

	ms.Unlock()
}

func (ms *Microservice) routeEvent(message Message, client *Client) error {
	if client.userId < 0 && message.Microservice != schema.PathMsAuthenticate {
		ms.DeleteClient(client)
		return nil
	}

	busMsg := schema.Message{
		FromMicroservice: ms.bus.Name,
		ToMicroservice:   message.Microservice,
		Method:           message.Method,
		Version:          message.Version,
		Body:             message.Body,
	}

	err := ms.bus.SendMessageInQueue(message.Microservice, busMsg)
	if err != nil {
		return err
	}

	return nil
}

func checkOrigin(request *http.Request) bool {
	return true
}
