#!/bin/bash

# Clear the terminal
clear

# Define ports to check and free
PORTS=(8000 8001 9000 9001)

# Function to clean up running Java processes
cleanup() {
  echo "Terminating all running Java programs..."
  for port in "${PORTS[@]}"; do
    PIDS=$(lsof -ti:$port)
    if [ -n "$PIDS" ]; then
      for PID in $PIDS; do
        echo "Terminating process $PID on port $port..."
        kill -9 "$PID"
      done
    fi
  done
  echo "All Java programs terminated."
  exit 0
}

# Trap Ctrl+C (SIGINT) to call cleanup
trap cleanup SIGINT

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
echo "Creating quickq.jar..."
jar cvf quickq.jar -C out . >> "$JAR_LOG" 2>&1

# Run the KVS Coordinator
echo "Starting KVS Coordinator on port 8000..." | tee -a "$LOG_FILE"
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Coordinator 8000 >> "$LOG_FILE" 2>&1 &
sleep 1

# Run the KVS Worker
echo "Starting KVS Worker on port 8001..." | tee -a "$LOG_FILE"
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8001 worker1 localhost:8000 >> "$LOG_FILE" 2>&1 &
sleep 1

# Run the Flame Coordinator
echo "Starting Flame Coordinator on port 9000..." | tee -a "$LOG_FILE"
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.Coordinator 9000 localhost:8000 >> "$LOG_FILE" 2>&1 &
sleep 1

# Run the Flame Worker
echo "Starting Flame Worker on port 9001..." | tee -a "$LOG_FILE"
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.Worker 9001 localhost:9000 >> "$LOG_FILE" 2>&1 &
sleep 1

# Tip message
echo -e "\nAll services started successfully. Press Ctrl+C to terminate all services." | tee -a "$LOG_FILE"

# Wait for all background processes
wait