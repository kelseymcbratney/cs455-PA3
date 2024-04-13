package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistSortedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        double loudness = Double.parseDouble(parts[1]);  // Assuming loudness is the second part
        context.write(new DoubleWritable(-loudness), new Text(parts[0]));  // Emitting negative for descending order
    }
}
