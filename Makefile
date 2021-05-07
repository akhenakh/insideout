.EXPORT_ALL_VARIABLES:

ifndef VERSION
VERSION := $(shell git describe --always --tags)
endif

DATE := $(shell date -u +%Y%m%d.%H%M%S)

BUILD_FLAGS = -trimpath -ldflags="-X main.version=$(VERSION)-$(DATE)"

CGO_ENABLED=0

ifneq (,$(wildcard ./vendor))
$(warning Found vendor directory setting go build flag to -mod vendor)
	BUILD_FLAGS += -mod vendor
endif

targets = insided indexer insidecli loadtester

.PHONY: all lint test insided insidecli indexer clean loadtester testnolint

all: test $(targets)

test: lint testnolint

CGO_ENABLED=1
testnolint:
	go test $(BUILD_FLAGS) -race ./...

lint:
	golangci-lint run

insided:
	cd cmd/insided && go build $(BUILD_FLAGS)

insidecli:
	cd cmd/insidecli && go build $(BUILD_FLAGS)

indexer:
	cd cmd/indexer && go build $(BUILD_FLAGS)

loadtester:
	cd cmd/loadtester && go build $(BUILD_FLAGS)

cmd/insided/grpc_health_probe: GRPC_HEALTH_PROBE_VERSION=v0.4.1
cmd/insided/grpc_health_probe:
	wget -qOcmd/insided/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
		chmod +x cmd/insided/grpc_health_probe

grpc_health_probe: cmd/insided/grpc_health_probe

indexer-countries: indexer
	rm -f ./cmd/insided/inside.db
	./cmd/indexer/indexer -dbPath=./cmd/insided/inside.db -filePath=testdata/ne_110m_admin_0_countries.geojson -outsideMinLevelCover=4 \
	-insideMinLevelCover=4 -insideMaxLevelCover=10 -outsideMaxLevelCover=10 -insideMaxLevelCover=32 -outsideMaxCellsCover=32

docker-image: insided grpc_health_probe indexer-countries
	cd ./cmd/insided/ && docker build . -t insideout-demo:${VERSION}
	docker tag insideout-demo:${VERSION} akhenakh/insideout-demo:latest

clean:
	rm -f cmd/indexer/indexer
	rm -f cmd/insided/insided
	rm -f cmd/insidecli/insidecli
	rm -f cmd/insided/grpc_health_probe
	rm -f cmd/loadtester/loadtester
