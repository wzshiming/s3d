#!/bin/bash
# Quick validation test for the compatibility test infrastructure
# This doesn't run the full mint suite, but validates that the scripts work

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Validating compatibility test infrastructure..."
echo ""

# Test 1: Check script syntax
echo "✓ Checking script syntax..."
bash -n run_mint.sh
bash -n parse_results.sh

# Test 2: Check dependencies are documented
echo "✓ Checking README exists..."
if [ ! -f README.md ]; then
    echo "✗ README.md not found"
    exit 1
fi

# Test 3: Test parse_results with sample data
echo "✓ Testing parse_results.sh with sample data..."
TMP_LOG=$(mktemp)
cat > "$TMP_LOG" << 'EOF'
{"name":"test-sdk","function":"TestFunction1","status":"PASS","duration":100}
{"name":"test-sdk","function":"TestFunction2","status":"PASS","duration":150}
{"name":"test-sdk","function":"TestFunction3","status":"FAIL","duration":200,"error":"Test error"}
EOF

OUTPUT=$(./parse_results.sh "$TMP_LOG" 2>&1)
rm "$TMP_LOG"

if echo "$OUTPUT" | grep -q "Total tests: 3"; then
    echo "  - Total count: OK"
else
    echo "  - Total count: FAILED"
    exit 1
fi

if echo "$OUTPUT" | grep -q "Passed: 2"; then
    echo "  - Pass count: OK"
else
    echo "  - Pass count: FAILED"
    exit 1
fi

if echo "$OUTPUT" | grep -q "Failed: 1"; then
    echo "  - Fail count: OK"
else
    echo "  - Fail count: FAILED"
    exit 1
fi

# Test 4: Verify Makefile target exists
echo "✓ Checking Makefile target..."
if grep -q "test-compatibility:" ../../Makefile; then
    echo "  - Makefile target exists: OK"
else
    echo "  - Makefile target missing: FAILED"
    exit 1
fi

# Test 5: Verify .gitignore entry
echo "✓ Checking .gitignore..."
if grep -q "test/compatibility/logs/" ../../.gitignore; then
    echo "  - .gitignore entry exists: OK"
else
    echo "  - .gitignore entry missing: FAILED"
    exit 1
fi

echo ""
echo "========================================="
echo "All validation checks passed! ✓"
echo "========================================="
echo ""
echo "The compatibility test infrastructure is ready to use."
echo "To run full compatibility tests, ensure Docker/Podman is installed and run:"
echo "  make test-compatibility"
echo ""
echo "Note: Full tests may take 10-30 minutes depending on MINT_MODE setting."
