package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(", ");
        if (parts.length >= 3) { // Ensure there are enough parts to avoid ArrayIndexOutOfBoundsException
            try {
                System.out.println(parts[0] + " " + parts[1] + " " + parts[2]);
                String artistID = parts[0].trim().split("\t")[1].trim(); // Correctly parse the artistID
                double loudness = Double.parseDouble(parts[2]); // Correctly parse the loudness as double
                context.write(new Text(artistID), new Text(String.valueOf(loudness)));
            } catch (NumberFormatException e) {
                // Log error to indicate a parsing failure
                System.err.println("Error parsing loudness in SongCountSortedMapper: " + parts[2]);
            }
        }
    }
}