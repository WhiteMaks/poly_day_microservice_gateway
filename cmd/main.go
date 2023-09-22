package main

import (
	"github.com/WhiteMaks/poly_day_bus_schema/schema"
	"github.com/WhiteMaks/poly_day_bus_schema/wbus"
	"log"
	"os"
	"poly_day_microservice_gateway/microservice"
)

var (
	rabbitConStr = os.Getenv("RABBIT_CONNECTING_STRING")
	appPort      = os.Getenv("APP_PORT")
)

func main() {
	bus := wbus.NewRabbit(schema.PathMsGateway)
	err := bus.Start(rabbitConStr)
	if err != nil {
		log.Fatalf("error start wbus: %v", err)
	}

	ms := microservice.NewMs(bus)
	err = ms.Start(":"+appPort, "/websocket")
	if err != nil {
		log.Fatalf("error start microservice: %v", err)
	}
}
