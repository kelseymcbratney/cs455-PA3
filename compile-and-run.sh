#!/bin/bash

# Clean the previous build artifacts
hadoop fs -rm -r /PA3/songCount

gradle clean

# Build the Gradle project
gradle build

# Copy the JAR file to the root directory
cp build/libs/JobRunner.jar .

hadoop jar JobRunner.jar /s/bach/l/under/kdmcb/cs455-PA3/analysis /s/bach/l/under/kdmcb/cs455-PA3/metadata /PA3/songCount
