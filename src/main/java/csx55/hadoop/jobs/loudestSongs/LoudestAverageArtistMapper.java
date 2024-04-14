package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // context.write(key, new Text(artistID + ", " + artistName + ", " + songTitle + ", " + loudness));
        String[] parts = value.toString().split(", ");
        String[] keyParts = key.toString().split("\\|");
        if (parts.length >= 3) { // Ensure there are enough parts to avoid ArrayIndexOutOfBoundsException
            try {
                String artistName = parts[1];
                String loudness = parts[2];
                context.write(new Text(keyParts[1]), new Text(artistName + ", " + loudness));
            } catch (NumberFormatException e) {
                // Log error to indicate a parsing failure
                System.err.println("Error parsing loudness in SongCountSortedMapper: " + parts[2]);
            }
        }
    }
}