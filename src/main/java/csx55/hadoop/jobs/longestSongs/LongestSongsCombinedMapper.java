package csx55.hadoop.jobs.longestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LongestSongsCombinedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length >= 6) {
            String artistID = parts[0].trim();
            String artistName = parts[1].trim();
            String songTitle = parts[2].trim();
            double duration = Double.parseDouble(parts[3].trim());

            // Emitting negative duration to sort in descending order
            context.write(new DoubleWritable(-duration), new Text(artistID + ", " + artistName + ", " + songTitle));
        }
    }
}
