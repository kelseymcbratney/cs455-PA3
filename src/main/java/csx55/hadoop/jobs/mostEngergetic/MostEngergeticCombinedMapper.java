package csx55.hadoop.jobs.mostEngergetic;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class MostEngergeticCombinedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length >= 4) {
            String artistID = parts[0].trim().split("\t")[1];
            String artistName = parts[1].trim();
            String songTitle = parts[2].trim();
            double dancability = Double.parseDouble(parts[3].trim());
            double energy = Double.parseDouble(parts[4].trim());

            // Emitting negative duration to sort in descending order
            context.write(new DoubleWritable(dancability + energy), new Text(artistID + ", " + artistName + ", " + songTitle));
        }
    }
}
