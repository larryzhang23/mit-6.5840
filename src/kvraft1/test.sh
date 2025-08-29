#!/usr/bin/env bash

fail=0
success=0
ROUNDS=500
CORES=$(sysctl -n hw.ncpu)   
JOBS=$(( CORES * 2 )) 

# directory for logs
LOG_DIR="./test_logs_4B"
mkdir -p "$LOG_DIR"

# run one test attempt
run_test() {
    local i=$1
    local logfile="$LOG_DIR/test_$i.log"

    echo "=========================="
    echo ">>> Running test round #$i (log: $logfile)"
    echo "=========================="

    if time go test -v -race -run 4B >"$logfile" 2>&1; then
        echo "success" > "$LOG_DIR/result_$i"
    else
        echo "fail" > "$LOG_DIR/result_$i"
    fi
}

export -f run_test
export LOG_DIR

# Run tests in parallel
seq 1 $ROUNDS | xargs -n1 -P"$JOBS" bash -c 'run_test "$@"' _

# Collect results
success=$(grep -l '^success$' "$LOG_DIR"/result_* 2>/dev/null | wc -l)
fail=$(grep -l '^fail$' "$LOG_DIR"/result_* 2>/dev/null | wc -l)
total=$((success + fail))

echo "=========================="
echo "Logs in: $LOG_DIR"
echo "Total runs (counted): $total"
echo "Succeeded:            $success"
echo "Failed:               $fail"
