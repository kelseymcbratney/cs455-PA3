#!/bin/bash

# Clean the previous build artifacts
gradle clean

# Build the Gradle project
gradle build

# Copy the JAR file to the root directory
cp build/libs/JobRunner.jar .

hadoop jar JobRunner.jar analysis.txt metadata.txt /PA3/output
