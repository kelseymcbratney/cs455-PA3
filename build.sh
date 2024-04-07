#!/bin/bash

# Set variables
PROJECT_DIR="/s/bach/l/under/kdmcb/cs455-PA3/src/main/java/csx55/hadoop"
HADOOP_HOME="/s/bach/l/under/kdmcb/cs455-PA3/"

# Navigate to the project directory
cd "$PROJECT_DIR" || exit

# Clean the previous build artifacts
./gradlew clean

# Build the Gradle project
./gradlew build

# Check if the build was successful
if [ $? -eq 0 ]; then
    echo "Build successful."
    # Copy the JAR file to the Hadoop directory
    cp build/libs/JobRunner.jar "$HADOOP_HOME"
    echo "Copied JAR to main directory."
else
    echo "Build failed. Please check your code and try again."
    exit 1
fi
