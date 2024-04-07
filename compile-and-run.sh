#!/bin/bash

# Clean the previous build artifacts
hadoop fs -rm -r /PA3/output

gradle clean

# Build the Gradle project
gradle build

# Copy the JAR file to the root directory
cp build/libs/JobRunner.jar .

hadoop jar JobRunner.jar csx55.hadoop.JobRunner /PA3/analysis.txt /PA3/metadata.txt /PA3/output
