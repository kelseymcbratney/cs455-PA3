#!/bin/bash
git pull

# Clean the previous build artifacts
hadoop fs -rm -r /PA3/output/
hadoop fs -rm -r /PA3/output_songCount/
hadoop fs -rm -r /PA3/output_loudestSongs/
hadoop fs -rm -r /PA3/output_loudestAverageArtist/
hadoop fs -rm -r /PA3/output_loudestAverageArtistSorted/
hadoop fs -rm -r /PA3/output_topHotttnesss
hadoop fs -rm -r /PA3/output_topHotttnesssSorted
hadoop fs -rm -r /PA3/output_topFadeIn
hadoop fs -rm -r /PA3/output_topFadeInCombined
hadoop fs -rm -r /PA3/output_topFadeInSorted
hadoop fs -rm -r /PA3/output_songLongestSong
hadoop fs -rm -r /PA3/output_songLongestSongCombined
hadoop fs -rm -r /PA3/output_mostEnergetic
hadoop fs -rm -r /PA3/output_mostEnergeticCombined
hadoop fs -rm -r /PA3/output_topFadeInFinal

rm JobRunner.jar

gradle clean

# Build the Gradle project
gradle build

# Copy the JAR file to the root directory
cp build/libs/JobRunner.jar .

hadoop jar JobRunner.jar file:///s/bach/l/under/kdmcb/cs455-PA3/analysis file:///s/bach/l/under/kdmcb/cs455-PA3/metadata /PA3/output 4
