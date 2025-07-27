#!/bin/bash
# The script that starts running project on EC2 instances

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
for i in $(seq 1 $TOTAL_WORKERS)
do
    WORKER_IP_VAR="KVS_FLAME_WORKER${i}_IP"
    WORKER_IP=${!WORKER_IP_VAR}
    echo "Worker $i IP: $WORKER_IP"
done

JAR_NAME="quickq.jar"
LOG_FILE="log.txt"
JAR_LOG="jar_creation.txt"

# Source the common functions
source ./aws_functions.sh

# Perform cleanup on remote EC2 instances
for i in $(seq 1 $TOTAL_WORKERS); do
    WORKER_IP_VAR="KVS_FLAME_WORKER${i}_IP"
    WORKER_IP=${!WORKER_IP_VAR}
    if [ -n "$WORKER_IP" ]; then
        echo "Cleaning up Worker $i on $WORKER_IP..."
        cleanup_remote "$WORKER_IP" "$CLEAN_WORKERS"
    fi
done
cleanup_local

echo "Starting the deployment process..."

# Compile Java files
echo "Compiling Java files..."
javac -cp src -d out src/cis5550/**/*.java

# Create the JAR file
echo "Creating $JAR_NAME..."
jar cvf "$JAR_NAME" -C out . >> "$JAR_LOG" 2>&1

# Copy JAR to all EC2 instances
echo "Copying JAR, frontend files, AND seed urls to EC2 instances..."
scp -i "$SSH_KEY" public/* ubuntu@"$SERVER_COORDINATOR_IP":"$REMOTE_PATH"/public
scp -i "$SSH_KEY" $JAR_NAME ubuntu@"$SERVER_COORDINATOR_IP":"$REMOTE_PATH"/
#scp -i "$SSH_KEY" seed_url.txt ubuntu@"$SERVER_COORDINATOR_IP":"$REMOTE_PATH"/

for i in $(seq 1 $TOTAL_WORKERS)
do
    WORKER_IP_VAR="KVS_FLAME_WORKER${i}_IP"
    WORKER_IP=${!WORKER_IP_VAR}
    if [ -n "$WORKER_IP" ]; then
        scp -i "$SSH_KEY" $JAR_NAME ubuntu@"$WORKER_IP":"$REMOTE_PATH"/
    fi
done

# Start KVS Coordinator, Flame Coordinator, and Backend Server on the shared node
echo "Starting KVS Coordinator, Flame Coordinator, and Backend Server on $SERVER_COORDINATOR_IP..."
ssh -i "$SSH_KEY" ubuntu@"$SERVER_COORDINATOR_IP" bash -s <<EOF
    java -Xmx56g -XX:MaxDirectMemorySize=2g -cp $REMOTE_PATH/$JAR_NAME cis5550.kvs.Coordinator 8000 >> $REMOTE_PATH/$LOG_FILE 2>&1 &
    java -Xmx56g -XX:MaxDirectMemorySize=2g -cp $REMOTE_PATH/$JAR_NAME cis5550.backend.Backend $SERVER_COORDINATOR_IP:8000 -r $REMOTE_PATH/public -http 8080 -https 8443 >> $REMOTE_PATH/$LOG_FILE 2>&1 &
    java -Xmx56g -XX:MaxDirectMemorySize=2g -cp $REMOTE_PATH/$JAR_NAME cis5550.flame.Coordinator 9000 $SERVER_COORDINATOR_IP:8000 >> $REMOTE_PATH/$LOG_FILE 2>&1 &
EOF
sleep 3

# Start workers on each node
echo "Starting workers on EC2 instances..."
for i in $(seq 1 $TOTAL_WORKERS)
do
    WORKER_IP_VAR="KVS_FLAME_WORKER${i}_IP"
    WORKER_IP=${!WORKER_IP_VAR}
    if [ -n "$WORKER_IP" ]; then
        echo "Starting KVS and Flame Worker $i on $WORKER_IP..."
        ssh -i "$SSH_KEY" ubuntu@"$WORKER_IP" bash -s <<EOF
            java -Xmx56g -XX:MaxDirectMemorySize=2g -cp $REMOTE_PATH/$JAR_NAME cis5550.kvs.Worker $((8000 + i)) worker$((i - 1))  $SERVER_COORDINATOR_IP:8000 $((i - 1)) $TOTAL_WORKERS >> $REMOTE_PATH/$LOG_FILE 2>&1 &
            java -Xmx56g -XX:MaxDirectMemorySize=2g -cp $REMOTE_PATH/$JAR_NAME cis5550.flame.Worker $((9000 + i)) $SERVER_COORDINATOR_IP:9000 >> $REMOTE_PATH/$LOG_FILE 2>&1 &
EOF
        sleep 3
    fi
done

echo -e "All services started successfully on respective EC2 instances.
Logs are being written to their respective files on each instance."