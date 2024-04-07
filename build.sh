#!/bin/bash

# Clean the previous build artifacts
gradle clean

# Build the Gradle project
gradle build

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
