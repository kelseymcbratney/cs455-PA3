package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(", ");
        if (parts.length >= 3) {
            try {
                String artistID = parts[0].trim();
                double loudness = Double.parseDouble(parts[2]);
                context.write(new Text(artistID), new DoubleWritable(loudness));
            } catch (NumberFormatException e) {
                // Handle parse error or invalid number
            }
        }
    }
}
