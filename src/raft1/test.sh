#!/usr/bin/env bash

fail=0
success=0
ROUNDS=100

for ((i=1; i<=ROUNDS; i++)); do
    echo "=========================="
    echo ">>> Running test round #$i"
    echo "=========================="
    
    if time go test -v -race -run 3B; then
        ((success++))
    else
        ((fail++))
    fi
done

echo "=========================="
echo "Total runs: $ROUNDS"
echo "Succeeded:  $success"
echo "Failed:     $fail"
