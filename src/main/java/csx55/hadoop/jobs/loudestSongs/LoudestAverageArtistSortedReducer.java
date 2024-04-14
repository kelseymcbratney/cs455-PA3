package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistSortedReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double originalLoudness = -key.get();  // Assuming loudness is stored as negative for sorting in descending order
        for (Text val : values) {
            String[] parts = val.toString().split(", ");
            if (parts.length >= 2) {
                String artistName = parts[0];
                // Output artistName with original loudness
                context.write(new Text(artistName), new DoubleWritable(originalLoudness));
            }
        }
    }
}
