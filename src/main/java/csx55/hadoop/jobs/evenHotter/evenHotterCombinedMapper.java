package csx55.hadoop.jobs.evenHotter;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class evenHotterCombinedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input from previous Reducer: key = songId, value = artistName|songTitle|hotttnesss
        String[] parts = value.toString().split("\\|");
        // (artistName + "|" + songTitle + "|" + hotttnesss + "|" + tempo + "|" + timeSignature + "|" + keySignature + "|" + energy + "|" + danceability + "|" + duration + "|" + loudness));
            if (parts.length == 10) {  // Ensure that there are exactly ten parts: artistName, songTitle, hotttnesss, tempo, timeSignature, keySignature, energy, danceability, duration, loudness
            String songTitle = parts[0];
            String artistName = parts[0];
            double hotttnesss;
            try {
                hotttnesss = Double.parseDouble(parts[1]);
                // Emit negative to sort in descending order
                context.write(new DoubleWritable(-hotttnesss), new Text(artistName + "|" + songTitle + "|" + parts[3] + "|" + parts[4] + "|" + parts[5] + "|" + parts[6] + "|" + parts[7] + "|" + parts[8] + "|" + parts[9]));
            } catch (NumberFormatException e) {
                System.err.println("Error parsing hotttnesss: " + parts[1]);
            }
        }
    }
}
