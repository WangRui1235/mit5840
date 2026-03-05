#!/bin/bash

RUNS=500
PARALLEL=48   # 默认 = CPU 核数，也可以 ./stress_test.sh 8 指定
LOGDIR="stress_logs"
SUMMARY="stress_summary.txt"
TMPDIR_RESULTS="stress_tmp_results"

mkdir -p "$LOGDIR" "$TMPDIR_RESULTS"
> "$SUMMARY"

echo "Starting $RUNS runs with $PARALLEL parallel workers."
echo "Logs in $LOGDIR/, summary in $SUMMARY"
echo "PID: $$"

# 运行单次测试的函数（子 shell 执行）
run_one() {
    i=$1
    logfile="$LOGDIR/run_$(printf '%04d' $i).txt"
    go test -race -run "3A|3B|3C|3D" > "$logfile" 2>&1
    exit_code=$?
    if [ $exit_code -eq 0 ]; then
        echo "PASS" > "$TMPDIR_RESULTS/$i"
    else
        echo "FAIL" > "$TMPDIR_RESULTS/$i"
    fi
    tail_line=$(tail -2 "$logfile" | tr '\n' ' ')
    echo "[$(cat $TMPDIR_RESULTS/$i)] Run $i/$RUNS: $tail_line"
}

export -f run_one
export LOGDIR RUNS TMPDIR_RESULTS

# 用 xargs 并行调度
seq 1 $RUNS | xargs -P "$PARALLEL" -I{} bash -c 'run_one "$@"' _ {} | tee -a "$SUMMARY"

# 汇总
pass=$(grep -c '^\[PASS\]' "$SUMMARY" || true)
fail=$(grep -c '^\[FAIL\]' "$SUMMARY" || true)

echo ""
echo "========================================" | tee -a "$SUMMARY"
echo "DONE: $pass PASS, $fail FAIL out of $RUNS" | tee -a "$SUMMARY"
echo "========================================" | tee -a "$SUMMARY"

rm -rf "$TMPDIR_RESULTS"
