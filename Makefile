.PHONY: all plugins relay-hosts routers build-rust build-python build-swift build-go \
       build-rust-relay-host build-python-relay-host build-swift-relay-host build-go-relay-host \
       build-rust-router build-swift-router \
       clean test test-matrix test-quick test-throughput test-relay test-multi-host

# Build everything
all: plugins relay-hosts routers

# Build all plugins
plugins: build-rust build-python build-swift build-go

# Build all relay hosts
relay-hosts: build-rust-relay-host build-python-relay-host build-swift-relay-host build-go-relay-host

# Build all routers
routers: build-rust-router build-swift-router

# --- Plugins ---

build-rust:
	@echo "Building Rust plugin..."
	cd src/capns_interop/plugins/rust && cargo build --release
	mkdir -p artifacts/build/rust
	cp src/capns_interop/plugins/rust/target/release/capns-interop-plugin-rust artifacts/build/rust/

build-python:
	@echo "Preparing Python plugin..."
	mkdir -p artifacts/build/python
	cp src/capns_interop/plugins/python/plugin.py artifacts/build/python/
	chmod +x artifacts/build/python/plugin.py

build-swift:
	@echo "Building Swift plugin..."
	cd src/capns_interop/plugins/swift && swift build -c release
	mkdir -p artifacts/build/swift
	cp src/capns_interop/plugins/swift/.build/release/capns-interop-plugin-swift artifacts/build/swift/

build-go:
	@echo "Building Go plugin..."
	cd src/capns_interop/plugins/go && go build -o capns-interop-plugin-go .
	mkdir -p artifacts/build/go
	cp src/capns_interop/plugins/go/capns-interop-plugin-go artifacts/build/go/

# --- Relay Hosts ---

build-rust-relay-host:
	@echo "Building Rust relay host..."
	cd src/capns_interop/hosts/rust-relay && cargo build --release
	mkdir -p artifacts/build/rust-relay
	rm -f artifacts/build/rust-relay/capns-interop-relay-host-rust
	cp src/capns_interop/hosts/rust-relay/target/release/capns-interop-relay-host-rust artifacts/build/rust-relay/

build-python-relay-host:
	@echo "Preparing Python relay host..."
	@# Python relay host runs from source — no build needed

build-swift-relay-host:
	@echo "Building Swift relay host..."
	cd src/capns_interop/hosts/swift-relay && swift build -c release
	mkdir -p artifacts/build/swift-relay
	rm -f artifacts/build/swift-relay/capns-interop-relay-host-swift
	cp src/capns_interop/hosts/swift-relay/.build/release/capns-interop-relay-host-swift artifacts/build/swift-relay/

build-go-relay-host:
	@echo "Building Go relay host..."
	cd src/capns_interop/hosts/go-relay && go build -o capns-interop-relay-host-go .
	mkdir -p artifacts/build/go-relay
	rm -f artifacts/build/go-relay/capns-interop-relay-host-go
	cp src/capns_interop/hosts/go-relay/capns-interop-relay-host-go artifacts/build/go-relay/

# --- Routers ---

build-rust-router:
	@echo "Building Rust router..."
	cd src/capns_interop/routers/rust && cargo build --release
	mkdir -p artifacts/build/rust-router
	rm -f artifacts/build/rust-router/capns-interop-router-rust
	cp src/capns_interop/routers/rust/target/release/capns-interop-router-rust artifacts/build/rust-router/

build-python-router:
	@echo "Preparing Python router..."
	@# TODO: Implement Python router

build-swift-router:
	@echo "Building Swift router..."
	cd src/capns_interop/routers/swift && swift build -c release
	mkdir -p artifacts/build/swift-router
	rm -f artifacts/build/swift-router/capns-interop-router-swift
	cp src/capns_interop/routers/swift/.build/release/capns-interop-router-swift artifacts/build/swift-router/

build-go-router:
	@echo "Building Go router..."
	@# TODO: Implement Go router

# --- Clean ---

clean:
	rm -rf artifacts/
	cd src/capns_interop/plugins/rust && cargo clean || true
	cd src/capns_interop/plugins/swift && swift package clean || true
	cd src/capns_interop/hosts/rust-relay && cargo clean || true
	cd src/capns_interop/hosts/swift-relay && swift package clean || true
	cd src/capns_interop/routers/rust && cargo clean || true
	cd src/capns_interop/routers/swift && swift package clean || true
	rm -f src/capns_interop/plugins/go/capns-interop-plugin-go
	rm -f src/capns_interop/hosts/go-relay/capns-interop-relay-host-go

# --- Test targets ---

test: all
	PYTHONPATH=src pytest tests/ -v

test-matrix: all
	PYTHONPATH=src pytest tests/test_host_matrix.py -v

test-throughput: all
	PYTHONPATH=src pytest tests/test_throughput_matrix.py -v -s --timeout=120

test-relay: all
	PYTHONPATH=src pytest tests/test_relay_interop.py -v --timeout=60

test-multi-host: all
	PYTHONPATH=src pytest tests/test_multi_host_interop.py -v --timeout=60

test-quick:
	PYTHONPATH=src pytest tests/ -v
