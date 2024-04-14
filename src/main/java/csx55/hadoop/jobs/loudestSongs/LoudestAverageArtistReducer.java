package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistName = "";
        double sum = 0;
        int count = 0;
        for (Text val : values) {
            String[] parts = val.toString().split(", ");
            if (parts.length < 4) { // Check if all parts are present
                // You might want to log this situation or handle it appropriately
                continue;
            }
            sum += Double.parseDouble(parts[3]); // parts[3] is the loudness
            artistName = parts[1]; // Assuming artistName is consistently in parts[1]
            count++;
        }
        double average = count > 0 ? sum / count : 0;
        context.write(key, new Text(artistName + ", " + average));
    }
}
