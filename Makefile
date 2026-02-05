.PHONY: build test test-integration test-e2e

# Build the server binary
build:
	go build ./cmd/s3d

# Run unit tests
test:
	go test -v ./pkg/... ./cmd/...

# Run integration tests
test-integration:
	go test -v ./test/integration/...

# Run e2e tests with AWS CLI
test-e2e: build
	./test/e2e/run_all.sh

# Run compatibility tests with minio/mint
test-mint-compatibility:
	./test/compatibility/mint_test.sh

# Update mint_compatibility.md from the latest test results
update-mint-compatibility:
	./test/compatibility/mint_results_to_readme.sh > ./test/compatibility/mint_compatibility.md
