#!/bin/bash

# Function to clean up remote EC2 instances
cleanup_remote() {
    local ip=$1
    local clean_workers=$2
    echo "Cleaning up old processes, files, and folders on $ip..."
    ssh -i "$SSH_KEY" ubuntu@"$ip" bash -s <<EOF
        pkill -f java && echo "Java processes killed."
        true > "$REMOTE_PATH/$LOG_FILE"

        rm -f "$REMOTE_PATH/$JAR_NAME"
        rm -f "$REMOTE_PATH/"*.jar

        rm -rf "$REMOTE_PATH"/public

        if [ "$clean_workers" = "true" ]; then
            rm -rf "$REMOTE_PATH/worker*"
            echo "Worker folders cleaned on $ip."
        fi

        echo "Cleanup completed on $ip."
EOF
}

# Function to clean up local files
cleanup_local() {
    rm -f "$JAR_LOG"
    rm -f ./*.jar
    echo "Local JAR and log files cleaned up."
}
