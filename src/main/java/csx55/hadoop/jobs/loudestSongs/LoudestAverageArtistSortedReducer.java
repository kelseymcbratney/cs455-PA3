package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistSortedReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double originalLoudness = -key.get();  // Reverting to original by negating the key
        for (Text val : values) {
            // Here 'val' already contains the artistName from Mapper, no need to split again
            context.write(val, new DoubleWritable(originalLoudness));
        }
    }
}
