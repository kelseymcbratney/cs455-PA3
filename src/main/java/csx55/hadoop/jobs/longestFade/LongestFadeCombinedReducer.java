package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;

public class LongestFadeCombinedReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalFade = 0.0;
        String artistName = null;

        // Iterate through the values to calculate total fade duration and get the artist name
        for (Text value : values) {
            String[] parts = value.toString().split(", ");
            double fadeDuration = Double.parseDouble(parts[0]);
            totalFade += fadeDuration;
            // The artist name is the same for all values, so we can extract it once
            if (artistName == null) {
                artistName = parts[1];
            }
        }

        // Emit the combined total fade duration for the artist ID along with the artist name
        context.write(key, new Text(totalFade + ", " + artistName));
    }
}
