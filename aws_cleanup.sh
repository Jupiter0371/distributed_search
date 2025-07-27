#!/bin/bash

# Check if the -c option is specified
CLEAN_WORKERS=false
while getopts "c" opt; do
  case $opt in
    c)
      CLEAN_WORKERS=true
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

# Load IPs from file
source EC2_IPs.txt

# Verify that the variables are loaded
echo "Total Workers: $TOTAL_WORKERS"
echo "Remote Path: $REMOTE_PATH"
echo "SSH Key file name: $SSH_KEY"
echo "Coordinator IP: $SERVER_COORDINATOR_IP"
for i in $(seq 1 $TOTAL_WORKERS); do
    WORKER_IP_VAR="KVS_FLAME_WORKER${i}_IP"
    WORKER_IP=${!WORKER_IP_VAR}
    echo "Worker $i IP: $WORKER_IP"
done

JAR_NAME="quickq.jar"
LOG_FILE="log.txt"
JAR_LOG="jar_creation.txt"

# Source the common functions
source ./aws_functions.sh

# Cleanup remote and local environments
cleanup_remote "$SERVER_COORDINATOR_IP" "$CLEAN_WORKERS"
for i in $(seq 1 $TOTAL_WORKERS); do
    WORKER_IP_VAR="KVS_FLAME_WORKER${i}_IP"
    WORKER_IP=${!WORKER_IP_VAR}
    if [ -n "$WORKER_IP" ]; then
        cleanup_remote "$WORKER_IP" "$CLEAN_WORKERS"
    fi
done
cleanup_local

echo "Finished cleaning up remote and local environments."