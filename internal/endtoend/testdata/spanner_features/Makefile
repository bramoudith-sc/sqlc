# Makefile for testing sqlc-generated code with Spanner emulator

# Start Spanner emulator
.PHONY: emulator-start
emulator-start:
	@echo "Starting Spanner emulator..."
	@docker run -d --name spanner-emulator \
		-p 9010:9010 -p 9020:9020 \
		gcr.io/cloud-spanner-emulator/emulator || echo "Emulator already running"

# Stop Spanner emulator
.PHONY: emulator-stop
emulator-stop:
	@echo "Stopping Spanner emulator..."
	@docker stop spanner-emulator || true
	@docker rm spanner-emulator || true

# Generate code with sqlc
.PHONY: generate
generate:
	@echo "Generating code with sqlc..."
	cd ../../../.. && ./sqlc generate -f internal/endtoend/testdata/spanner_features/sqlc.yaml

# Run tests with emulator
.PHONY: test
test:
	@echo "Running tests with Spanner emulator..."
	cd go && SPANNER_EMULATOR_HOST=localhost:9010 go test -tags=emulator -v

# Run all: start emulator, generate code, test, stop emulator
.PHONY: all
all: emulator-start generate test emulator-stop

# Clean generated files
.PHONY: clean
clean:
	@echo "Cleaning generated files..."
	rm -f go/*.sql.go

# Help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  emulator-start  - Start Spanner emulator in Docker"
	@echo "  emulator-stop   - Stop and remove Spanner emulator container"
	@echo "  generate        - Generate Go code using sqlc"
	@echo "  test            - Run tests with Spanner emulator"
	@echo "  all             - Run complete test suite (start emulator, generate, test, stop)"
	@echo "  clean           - Remove generated files"
	@echo "  help            - Show this help message"