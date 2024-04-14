package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistSortedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(", ");
        if (parts.length >= 2) {  // Make sure there are enough parts
            try {
                double loudness = Double.parseDouble(parts[1]);  // Assuming loudness is the second part
                String artistName = parts[0];
                context.write(new DoubleWritable(-loudness), new Text(artistName));  // Emit negative loudness for descending sort
            } catch (NumberFormatException e) {
                System.err.println("Error parsing loudness for artist " + parts[0] + ": " + parts[1]);
            }
        }
    }
}
