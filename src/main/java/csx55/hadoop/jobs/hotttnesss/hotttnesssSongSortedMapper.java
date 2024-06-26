package csx55.hadoop.jobs.hotttnesss;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class hotttnesssSongSortedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input from previous Reducer: key = songId, value = artistName|songTitle|hotttnesss
        String[] parts = value.toString().split("\\|");
        if (parts.length == 3) {  // Ensure that there are exactly three parts: artistName, songTitle, hotttnesss
            String artistName = parts[0];
            String songTitle = parts[1];
            double hotttnesss;
            try {
                hotttnesss = Double.parseDouble(parts[2]);
                // Emit negative to sort in descending order
                context.write(new DoubleWritable(-hotttnesss), new Text(artistName + "|" + songTitle));
            } catch (NumberFormatException e) {
                System.err.println("Error parsing hotttnesss: " + parts[2]);
            }
        }
    }
}
