package main

import (
	"context"
	"log"
	"time"

	//_ "github.com/mbobakov/grpc-consul-resolver"
	"github.com/namsral/flag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"

	"github.com/akhenakh/insideout/insidesvc"
)

var (
	insideURI = flag.String("insideURI", "localhost:9200", "insided grpc URI")
	lat       = flag.Float64("lat", 48.8, "Lat")
	lng       = flag.Float64("lng", 2.2, "Lng")
	count     = flag.Int("count", 1, "how many requests to perform")
)

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*insideURI,
		grpc.WithInsecure(),
		grpc.WithBalancerName(roundrobin.Name), //nolint:staticcheck
	)
	if err != nil {
		log.Fatal(err)
	}

	c := insidesvc.NewInsideClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < *count; i++ {
		resps, err := c.Within(ctx, &insidesvc.WithinRequest{
			Lat: *lat,
			Lng: *lng,
		})
		if err != nil {
			log.Fatal(err)
		}

		for _, fresp := range resps.Responses {
			log.Printf("Found in ID: %d properties: %s\n", fresp.Id, fresp.Feature.Properties)
		}
	}
}
