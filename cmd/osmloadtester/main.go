package main

import (
	"context"
	"fmt"
	stdlog "log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// _ "net/http/pprof" .

	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/namsral/flag"
	"github.com/rcrowley/go-metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"

	"github.com/akhenakh/insideout/gen/go/osminsidesvc/v1"
	"github.com/akhenakh/insideout/loglevel"
)

const appName = "loadtester"

var (
	logLevel     = flag.String("logLevel", "INFO", "DEBUG|INFO|WARN|ERROR")
	testDuration = flag.Duration("testDuration", 0, "performs the test for duration, 0 = infinite")
	insideURI    = flag.String("insideURI", "localhost:9200", "insided grpc URI")
	latMin       = flag.Float64("latMin", 49.10, "Lat min")
	lngMin       = flag.Float64("lngMin", -1.10, "Lng min")
	latMax       = flag.Float64("latMax", 46.63, "Lat max")
	lngMax       = flag.Float64("lngMax", 5.5, "Lng max")
)

func main() {
	flag.Parse()

	exitcode := 0
	defer func() { os.Exit(exitcode) }()

	// pprof
	// go func() {
	// 	stdlog.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "caller", log.Caller(5), "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "app", appName)
	logger = loglevel.NewLevelFilterFromString(logger, *logLevel)

	stdlog.SetOutput(log.NewStdlibAdapter(logger))

	rand.Seed(time.Now().UnixNano())

	conn, err := grpc.Dial(*insideURI,
		grpc.WithInsecure(),
		grpc.WithBalancerName(roundrobin.Name), //nolint:staticcheck
	)
	if err != nil {
		level.Error(logger).Log("msg", "error dialing", "error", err)

		exitcode = 1

		return
	}

	c := osminsidesvc.NewOSMInsideServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())

	if *testDuration > 0 {
		ctx, cancel = context.WithTimeout(ctx, *testDuration)
	}

	// catch termination
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	defer signal.Stop(interrupt)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		tm := metrics.NewTimer()

		req := &osminsidesvc.WithinRequest{
			Lat: *latMin,
			Lng: *latMax,
		}

		for {
			ctx, rcancel := context.WithTimeout(ctx, 200*time.Millisecond)

			t := time.Now()
			lat := *latMin + rand.Float64()*(*latMax-*latMin) // nolint: gosec
			lng := *lngMin + rand.Float64()*(*lngMax-*lngMin) // nolint: gosec
			req.Lat = lat
			req.Lng = lng

			resps, err := c.Within(ctx, req)
			if err != nil {
				level.Error(logger).Log("msg", "error with request", "error", err)
				rcancel()
				cancel()

				break
			}

			tm.UpdateSince(t)

			rcancel()

			level.Debug(logger).Log(
				"msg", "found features",
				"fids", resps.Fids,
				"lat", lat,
				"lng", lng,
			)
		}

		msg := fmt.Sprintf("count %d rate mean %.0f/s rate1 %.0f/s 99p %.0f\n",
			tm.Count(), tm.RateMean(), tm.Rate1(), tm.Percentile(99.0))
		level.Info(logger).Log("msg", msg)
	}()

	select {
	case <-interrupt:
		cancel()

		break
	case <-ctx.Done():
		break
	}

	wg.Wait()
}
