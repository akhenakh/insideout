package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	stdlog "log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akhenakh/insideout/gen/go/osminsidesvc/v1"
	"github.com/akhenakh/insideout/loglevel"
	"github.com/akhenakh/insideout/storage/bbolt"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

const appName = "osminsided"

var (
	version = "no version from LDFLAGS"

	logLevel        = flag.String("logLevel", "INFO", "DEBUG|INFO|WARN|ERROR")
	dbPath          = flag.String("dbPath", "inside.db", "Database path")
	httpMetricsPort = flag.Int("httpMetricsPort", 8088, "http port")
	httpAPIPort     = flag.Int("httpAPIPort", 8080, "http API port")
	grpcPort        = flag.Int("grpcPort", 9200, "gRPC API port")
	healthPort      = flag.Int("healthPort", 6666, "grpc health port")

	grpcHealthServer  *grpc.Server
	grpcServer        *grpc.Server
	httpMetricsServer *http.Server
)

func main() {
	flag.Parse()

	exitcode := 0
	defer func() { os.Exit(exitcode) }()

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "caller", log.Caller(5), "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "app", appName)
	logger = loglevel.NewLevelFilterFromString(logger, *logLevel)

	stdlog.SetOutput(log.NewStdlibAdapter(logger))

	level.Info(logger).Log("msg", "Starting app", "version", version)

	storage, clean, err := bbolt.NewOSMROStorage(*dbPath, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to open storage", "error", err, "db_path", *dbPath)

		exitcode = 1

		return
	}

	defer clean()

	infos, err := storage.LoadIndexInfos()
	if err != nil {
		level.Error(logger).Log("msg", "failed to read infos", "error", err)

		exitcode = 1

		return
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// catch termination
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	defer signal.Stop(interrupt)

	g, ctx := errgroup.WithContext(ctx)

	// gRPC Health Server
	healthServer := health.NewServer()

	g.Go(func() error {
		grpcHealthServer = grpc.NewServer()

		healthpb.RegisterHealthServer(grpcHealthServer, healthServer)

		haddr := fmt.Sprintf(":%d", *healthPort)
		hln, err := net.Listen("tcp", haddr)
		if err != nil {
			level.Error(logger).Log("msg", "gRPC Health server: failed to listen", "error", err)
			os.Exit(2)
		}
		level.Info(logger).Log("msg", fmt.Sprintf("gRPC health server listening at %s", haddr))

		return grpcHealthServer.Serve(hln)
	})

	// server
	server, err := NewServer(ctx, logger, storage, healthServer)
	if err != nil {
		level.Error(logger).Log("msg", "can't get a working server", "error", err)

		exitcode = 1

		return
	}

	// web server metrics
	g.Go(func() error {
		httpMetricsServer = &http.Server{
			Addr:         fmt.Sprintf(":%d", *httpMetricsPort),
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second, // keep a long timeout for pprof
		}
		level.Info(logger).Log("msg", fmt.Sprintf("HTTP Metrics server listening at :%d", *httpMetricsPort))

		versionGauge.WithLabelValues(version).Add(1)
		dataVersionGauge.WithLabelValues(
			fmt.Sprintf("%s %s", infos.Filename, infos.IndexTime.Format(time.RFC3339)),
		).Add(1)

		// Register Prometheus metrics handler.
		http.Handle("/metrics", promhttp.Handler())

		if err := httpMetricsServer.ListenAndServe(); errors.Is(err, http.ErrServerClosed) {
			return err
		}

		return nil
	})

	// gRPC server
	g.Go(func() error {
		addr := fmt.Sprintf(":%d", *grpcPort)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			level.Error(logger).Log("msg", "gRPC server: failed to listen", "error", err)
			os.Exit(2)
		}

		grpc_prometheus.EnableHandlingTimeHistogram()

		grpcServer = grpc.NewServer(
			// MaxConnectionAge is just to avoid long connection, to facilitate load balancing
			// MaxConnectionAgeGrace will torn them, default to infinity
			grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 5 * time.Minute}),
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
				grpc_opentracing.StreamServerInterceptor(),
				grpc_prometheus.StreamServerInterceptor,
			)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				grpc_opentracing.UnaryServerInterceptor(),
				grpc_prometheus.UnaryServerInterceptor,
			)),
		)
		osminsidesvc.RegisterOSMInsideServiceServer(grpcServer, server)

		return grpcServer.Serve(ln)
	})

	healthServer.SetServingStatus(fmt.Sprintf("grpc.health.v1.%s", appName), healthpb.HealthCheckResponse_SERVING)
	level.Info(logger).Log("msg", "serving status to SERVING")

	select {
	case <-interrupt:
		cancel()

		break
	case <-ctx.Done():
		break
	}

	level.Warn(logger).Log("msg", "received shutdown signal")

	healthServer.SetServingStatus(fmt.Sprintf("grpc.health.v1.%s", appName), healthpb.HealthCheckResponse_NOT_SERVING)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if httpMetricsServer != nil {
		_ = httpMetricsServer.Shutdown(shutdownCtx)
	}

	if grpcServer != nil {
		grpcServer.GracefulStop()
	}

	if grpcHealthServer != nil {
		grpcHealthServer.GracefulStop()
	}

	err = g.Wait()
	if err != nil {
		level.Error(logger).Log("msg", "server returning an error", "error", err)

		exitcode = 1

		return
	}
}
