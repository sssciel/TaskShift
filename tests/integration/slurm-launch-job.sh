#!/usr/bin/env bash
#
# Test version of slurm-launch-job.sh — calls fake scontrol
#

set -euo pipefail

FAKE_CONTROL="${FAKE_SLURM_CONTROL_BIN:-tests/integration/fake_scontrol.sh}"

JOB_ID="${TASKSHIFT_JOB_ID:-}"
if [ -z "$JOB_ID" ]; then
    echo "error: TASKSHIFT_JOB_ID is not set" >&2
    exit 1
fi

if ! [[ "$JOB_ID" =~ ^[0-9]+$ ]]; then
    echo "error: TASKSHIFT_JOB_ID is not a valid integer: '$JOB_ID'" >&2
    exit 1
fi

TARGET_QOS="${TASKSHIFT_QOS:-taskshift}"

echo "taskshift: updating job $JOB_ID → QoS=$TARGET_QOS"

"$FAKE_CONTROL" update JobId="$JOB_ID" QOS="$TARGET_QOS"

echo "taskshift: job $JOB_ID launched successfully"
exit 0
