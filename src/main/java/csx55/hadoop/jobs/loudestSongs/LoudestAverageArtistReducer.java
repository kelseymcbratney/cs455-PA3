package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        String artistName = ""; // Variable to store artist name

        for (Text val : values) {
            String[] parts = val.toString().split(", ");
            if (parts.length >= 2) {
                artistName = parts[0]; // Assume artistName is stored in parts[0]
                double loudness = Double.parseDouble(parts[1]); // Loudness is in parts[1]
                sum += loudness;
                count++;
            }
        }

        double average = count > 0 ? sum / count : 0;
        if (!artistName.isEmpty()) {
            context.write(key, new Text(artistName + ", " + average));
        }
    }
}
