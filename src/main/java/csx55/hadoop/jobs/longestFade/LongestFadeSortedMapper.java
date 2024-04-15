package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LongestFadeSortedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input from previous job: ID, artistID, artistName, songTitle, fadeTime
        String[] parts = value.toString().split(",");
        if (parts.length == 5) {  // Ensure we have exactly five parts
            String artistName = parts[2].trim();
            String songTitle = parts[3].trim();
            double fadeTime;
            try {
                fadeTime = Double.parseDouble(parts[4].trim());
                // Emit negative to sort in descending order
                context.write(new DoubleWritable(-fadeTime), new Text(artistName + " - " + songTitle));
            } catch (NumberFormatException e) {
                System.err.println("Error parsing fade time: " + parts[4]);
            }
        }
    }
}
