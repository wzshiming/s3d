#!/bin/bash

# Parse mint test results and generate a compatibility report
# This script parses the log.json file from minio/mint and displays
# which S3 features are compatible and which are not

LOG_FILE=$1

if [ ! -f "$LOG_FILE" ]; then
    echo "Error: Log file not found: $LOG_FILE"
    exit 1
fi

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if jq is available
if ! command -v jq &> /dev/null; then
    echo -e "${YELLOW}Warning: jq not found, using basic parsing${NC}"
    USE_JQ=false
else
    USE_JQ=true
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}S3D Compatibility Test Results${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Parse results
if [ "$USE_JQ" = true ]; then
    # Count total tests
    TOTAL=$(jq -s 'length' "$LOG_FILE")
    PASSED=$(jq -s '[.[] | select(.status == "PASS")] | length' "$LOG_FILE")
    FAILED=$(jq -s '[.[] | select(.status == "FAIL")] | length' "$LOG_FILE")
    NA=$(jq -s '[.[] | select(.status == "NA")] | length' "$LOG_FILE")
    
    echo -e "${BLUE}Summary:${NC}"
    echo -e "  Total tests: ${TOTAL}"
    echo -e "  ${GREEN}Passed: ${PASSED}${NC}"
    echo -e "  ${RED}Failed: ${FAILED}${NC}"
    echo -e "  ${YELLOW}Not Applicable: ${NA}${NC}"
    
    if [ $TOTAL -gt 0 ]; then
        PASS_RATE=$(awk "BEGIN {printf \"%.1f\", ($PASSED/$TOTAL)*100}")
        echo -e "  Pass rate: ${PASS_RATE}%\n"
    fi
    
    # Group by SDK/tool
    echo -e "${BLUE}Results by SDK/Tool:${NC}"
    jq -r -s 'group_by(.name) | .[] | 
        "\(.[]|.name|@text) - Total: \(length), Passed: \([.[]|select(.status=="PASS")]|length), Failed: \([.[]|select(.status=="FAIL")]|length)"' \
        "$LOG_FILE" | while read -r line; do
        if [[ $line == *"Failed: 0"* ]]; then
            echo -e "  ${GREEN}✓${NC} $line"
        else
            echo -e "  ${YELLOW}⚠${NC} $line"
        fi
    done
    
    # Show failed tests
    if [ $FAILED -gt 0 ]; then
        echo -e "\n${RED}Failed Tests:${NC}"
        jq -r -s '.[] | select(.status == "FAIL") | 
            "  [\(.name)] \(.function // "unknown")\n    Error: \(.error // .alert // "No error message")"' \
            "$LOG_FILE" | head -50
        
        if [ $FAILED -gt 10 ]; then
            echo -e "\n  ${YELLOW}... showing first 10 failures, see log file for all failures${NC}"
        fi
    fi
    
    # Show compatible features
    echo -e "\n${GREEN}Compatible Features (functions that passed):${NC}"
    jq -r -s '.[] | select(.status == "PASS") | .function // "unknown"' "$LOG_FILE" | \
        sort -u | head -20 | while read -r func; do
        echo -e "  ✓ $func"
    done
    
    TOTAL_PASS_FUNCS=$(jq -r -s '.[] | select(.status == "PASS") | .function // "unknown"' "$LOG_FILE" | sort -u | wc -l)
    if [ $TOTAL_PASS_FUNCS -gt 20 ]; then
        echo -e "  ${YELLOW}... and $((TOTAL_PASS_FUNCS - 20)) more${NC}"
    fi
    
    # Show incompatible features
    if [ $FAILED -gt 0 ]; then
        echo -e "\n${RED}Incompatible Features (functions that failed):${NC}"
        jq -r -s '.[] | select(.status == "FAIL") | .function // "unknown"' "$LOG_FILE" | \
            sort -u | head -20 | while read -r func; do
            echo -e "  ✗ $func"
        done
        
        TOTAL_FAIL_FUNCS=$(jq -r -s '.[] | select(.status == "FAIL") | .function // "unknown"' "$LOG_FILE" | sort -u | wc -l)
        if [ $TOTAL_FAIL_FUNCS -gt 20 ]; then
            echo -e "  ${YELLOW}... and $((TOTAL_FAIL_FUNCS - 20)) more${NC}"
        fi
    fi
    
else
    # Basic parsing without jq
    echo -e "${YELLOW}Basic parsing mode (install jq for detailed analysis)${NC}\n"
    
    TOTAL=$(grep -c '"status"' "$LOG_FILE" || echo "0")
    PASSED=$(grep -c '"status":"PASS"' "$LOG_FILE" || echo "0")
    FAILED=$(grep -c '"status":"FAIL"' "$LOG_FILE" || echo "0")
    
    echo -e "${BLUE}Summary:${NC}"
    echo -e "  Total tests: ${TOTAL}"
    echo -e "  ${GREEN}Passed: ${PASSED}${NC}"
    echo -e "  ${RED}Failed: ${FAILED}${NC}\n"
    
    if [ $TOTAL -gt 0 ]; then
        PASS_RATE=$(awk "BEGIN {printf \"%.1f\", ($PASSED/$TOTAL)*100}")
        echo -e "  Pass rate: ${PASS_RATE}%\n"
    fi
    
    echo -e "For detailed results, install jq: apt-get install jq / brew install jq"
fi

echo -e "\n${BLUE}========================================${NC}"
