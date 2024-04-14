package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistSortedReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // context.write(key, new Text(artistID + ", " + artistName + ", " + songTitle + ", " + loudness));
        double originalLoudness = -key.get();  // Reverting to original by negating the key
        for (Text val : values) {
            String parts[] = val.toString().split(", ");
            System.out.println(parts[0] + " " + parts[1] + " " + parts[2]);
            String artistName = parts[1];
            context.write(val, new Text(artistName + ", " + originalLoudness));
        }
    }
}
