.PHONY: build test test-integration test-e2e test-mint-compatibility test-s3tests-compatibility test-pjdfstest-compatibility update-mint-compatibility update-s3tests-compatibility update-pjdfstest-compatibility

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

# Run compatibility tests with ceph/s3-tests
test-s3tests-compatibility:
	./test/compatibility/s3tests_test.sh

# Update s3tests_compatibility.md from the latest test results
update-s3tests-compatibility:
	./test/compatibility/s3tests_results_to_readme.sh > ./test/compatibility/s3tests_compatibility.md

# Run compatibility tests with pjd/pjdfstest
test-pjdfstest-compatibility:
	./test/compatibility/pjdfstest_test.sh

# Update pjdfstest_compatibility.md from the latest test results
update-pjdfstest-compatibility:
	./test/compatibility/pjdfstest_results_to_readme.sh > ./test/compatibility/pjdfstest_compatibility.md
