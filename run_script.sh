#!/bin/bash

# Clear the terminal
clear

# Function to clean up running Java processes
cleanup() {
  echo "Terminating all running processes..."
  if [[ -n "${PIDS[*]}" ]]; then
    for PID in "${PIDS[@]}"; do
      echo "Terminating process $PID..."
      kill -9 "$PID"
    done
  fi
  echo "All processes terminated."
  exit 0
}

# Trap Ctrl+C (SIGINT) to call cleanup
trap cleanup SIGINT

# Parse command-line arguments
CLEAN_WORKERS=false
NUM_FLAME_WORKERS=1
NUM_KVS_WORKERS=1
JAR_NAME="quickq.jar"

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -h)
      echo "Usage: $0 [-c] [-kn <number>] [-fn <number>] [-j <file>] [-h]"
      echo "Options:"
      echo "  -h: Display this help message"
      echo "  -c: Clean the worker directories before starting the worker"
      echo "  -kn <number>: Start the specified number of KVS Workers (default: 1)"
      echo "  -fn <number>: Start the specified number of Flame Workers (default: 1)"
      echo "  -j <file>: Specify the name of the JAR file to create (default: quickq)"
      exit 0
      ;;
    -c)
      CLEAN_WORKERS=true
      shift
      ;;
    -kn)
      if [[ $2 =~ ^[0-9]+$ ]]; then
        NUM_KVS_WORKERS=$2
        shift 2
      else
        echo "Error: -kn option requires a numeric argument."
        exit 1
      fi
      ;;
    -fn)
      if [[ $2 =~ ^[0-9]+$ ]]; then
        NUM_FLAME_WORKERS=$2
        shift 2
      else
        echo "Error: -fn option requires a numeric argument."
        exit 1
      fi
      ;;
    -j)
      if [[ -n $2 ]]; then
        JAR_NAME="$2.jar"
        shift 2
      else
        echo "Error: -j option requires a file name."
        exit 1
      fi
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [-c] [-kn <number>] [-fn <number>] [-j <file>] [-h]"
      echo "Options:"
      echo "  -h: Display this help message"
      echo "  -c: Clean the worker directories before starting the worker"
      echo "  -kn <number>: Start the specified number of KVS Workers (default: 1)"
      echo "  -fn <number>: Start the specified number of Flame Workers (default: 1)"
      echo "  -j <file>: Specify the name of the JAR file to create (default: quickq)"
      exit 1
      ;;
  esac
done

# Define the output log files
JAR_LOG="jar_creation.txt"
LOG_FILE="complete_log.txt"
GENERAL_LOG="log.txt"

# Clear the log files at the start
true > "$JAR_LOG"
true > "$LOG_FILE"
true > "$GENERAL_LOG"

# Compile Java files
echo "Compiling Java files..."
javac -cp src -d out src/cis5550/**/*.java

# Create the JAR file
echo "Creating $JAR_NAME..."
jar cvf "$JAR_NAME" -C out . >> "$JAR_LOG" 2>&1

# Run the KVS Coordinator
echo "Starting KVS Coordinator on port 8000..." | tee -a "$LOG_FILE"
java -Xmx6g -cp out cis5550.kvs.Coordinator 8000 >> "$LOG_FILE" 2>&1 &
sleep 1
PIDS+=($!)

# Clear the worker directories if specified
if [ "$CLEAN_WORKERS" == true ]; then
  echo "Cleaning directories for all workers..."
  rm -rf worker*
fi

# Run the specified number of KVS Workers
echo "Starting $NUM_KVS_WORKERS KVS Workers..."
for ((i=0; i<NUM_KVS_WORKERS; i++)); do
  PORT=$((8001 + i))
  echo "Starting KVS Worker on port $PORT..." | tee -a "$LOG_FILE"
  java -Xmx6g -cp out cis5550.kvs.Worker $PORT worker$i localhost:8000 $i "$NUM_KVS_WORKERS">> "$LOG_FILE" 2>&1 &
  sleep 1
  PIDS+=($!)
done

# Run the Flame Coordinator
echo "Starting Flame Coordinator on port 9000..." | tee -a "$LOG_FILE"
java -Xmx6g -cp out cis5550.flame.Coordinator 9000 localhost:8000 >> "$LOG_FILE" 2>&1 &
sleep 1
PIDS+=($!)

# Run the specified number of Flame Workers
echo "Starting $NUM_FLAME_WORKERS Flame Workers..."
for ((i=0; i<NUM_FLAME_WORKERS; i++)); do
  PORT=$((9001 + i))
  echo "Starting Flame Worker on port $PORT..." | tee -a "$LOG_FILE"
  java -Xmx6g -cp out cis5550.flame.Worker $PORT localhost:9000 >> "$LOG_FILE" 2>&1 &
  sleep 1
  PIDS+=($!)
done

# Run the Backend Server
echo "Starting Backend Server on HTTP port 8080, HTTPS port 8443..." | tee -a "$LOG_FILE"
java -Xmx6g -cp out cis5550.backend.Backend localhost:8000 -http 8080 -https 8443 >> "$LOG_FILE" 2>&1 &
sleep 1
PIDS+=($!)

echo -e "\nAll services started successfully. Press Ctrl+C to terminate all services." | tee -a "$LOG_FILE"

wait