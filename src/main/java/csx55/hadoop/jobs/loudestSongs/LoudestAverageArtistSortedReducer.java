package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistSortedReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(val, key);
        }
    }
}
