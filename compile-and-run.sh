#!/bin/bash
git pull

# Clean the previous build artifacts
hadoop fs -rm -r /PA3/output/
hadoop fs -rm -r /PA3/output_songCount/
hadoop fs -rm -r /PA3/output_loudestSongs/
hadoop fs -rm -r /PA3/output_loudestAverageArtist/
hadoop fs -rm -r /PA3/output_loudestAverageArtistSorted/

rm JobRunner.jar

gradle clean

# Build the Gradle project
gradle build

# Copy the JAR file to the root directory
cp build/libs/JobRunner.jar .

hadoop jar JobRunner.jar file:///s/bach/l/under/kdmcb/cs455-PA3/analysis file:///s/bach/l/under/kdmcb/cs455-PA3/metadata /PA3/output 0
