package csx55.hadoop.jobs.evenHotter;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class evenHotterCombinedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private double hotttnesss;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input from previous Reducer: key = songId, value = artistName|songTitle|hotttnesss
        String[] parts = value.toString().split("\\|");
        // (artistName + "|" + songTitle + "|" + hotttnesss + "|" + tempo + "|" + timeSignature + "|" + keySignature + "|" + energy + "|" + danceability + "|" + duration + "|" + loudness));
            if (parts.length == 10) {  // Ensure that there are exactly ten parts: artistName, songTitle, hotttnesss, tempo, timeSignature, keySignature, energy, danceability, duration, loudness
            String songTitle = parts[0].split("\\t")[1];
            String songId = parts[0].split("\\t")[0];
            System.out.println("parts[1]: " + parts[1]);
            System.out.println("parts[1] type: " + Arrays.toString(parts));
            try {
                hotttnesss = Double.parseDouble(parts[1]);
                // Emit negative to sort in descending order
            } catch (NumberFormatException e) {
                System.err.println("Error parsing hotttnesss: " + parts[1]);
            }
                context.write(new DoubleWritable(-hotttnesss), new Text(songId + "|" + songTitle + "|" + parts[3] + "|" + parts[4] + "|" + parts[5] + "|" + parts[6] + "|" + parts[7] + "|" + parts[8] + "|" + parts[9]));
        }
    }
}
