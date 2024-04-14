package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(", ");
        if (parts.length >= 2) { // Ensure there are enough parts to avoid ArrayIndexOutOfBoundsException
            String artistID = parts[0].trim();
            String artistName = parts[1].trim();
            String loudness = parts[2].trim(); // Assuming the loudness is at index 2 based on your output format
            context.write(new Text(artistID), new Text(artistName + ", " + loudness));
        }
    }
}
