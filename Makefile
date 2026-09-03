BIN := bin
PKGS := ./...

.PHONY: all build test race vet fmt lint demo bench clean docker

all: fmt vet test build

build:
	@mkdir -p $(BIN)
	go build -o $(BIN)/meshd    ./cmd/meshd
	go build -o $(BIN)/meshcp   ./cmd/meshcp
	go build -o $(BIN)/backend  ./cmd/backend
	go build -o $(BIN)/loadgen  ./cmd/loadgen

test:
	go test $(PKGS)

# The race detector is not optional here: the proxy swaps live configuration
# under concurrent traffic, which is exactly the shape of bug it exists to find.
race:
	go test $(PKGS) -race -timeout 300s

vet:
	go vet $(PKGS)

fmt:
	gofmt -w .

# Replay a specific simulation seed. Usage: make seed SEED=4471
seed:
	go test ./internal/simulation/ -run TestSameSeedReplaysIdentically -v

demo: build
	./scripts/demo.sh

clean:
	rm -rf $(BIN) bench/*.csv

docker:
	docker build -f deploy/Dockerfile -t meshd:dev .
