#!/usr/bin/env sh
# Fake scontrol command for TaskShift integration tests
# Logs all invocations for test verification

LOG_FILE="${FAKE_SLURM_CONTROL_LOG:-/tmp/fake_scontrol.log}"

# Append the full command invocation to the log
echo "scontrol $*" >> "$LOG_FILE"

# Exit success
exit 0
