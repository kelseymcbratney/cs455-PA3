package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LongestFadeSortedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim(); // Trim any leading or trailing whitespace

        // Split the line by tab delimiter
        String[] parts = line.split("\t");
        if (parts.length < 2) {
            // Log to Hadoop's system logs if input is not in expected format
            System.err.println("Invalid line format: " + line);
            return;
        }

        try {
            // Parse the fade time as a double
            double fadeTime = Double.parseDouble(parts[0].trim());

            // The rest of the line will be considered as artist details
            String artistID = parts[1].trim();

            // Write to context, negate fade time to sort in descending order
            context.write(new DoubleWritable(-fadeTime), new Text(artistID));
        } catch (NumberFormatException e) {
            // Catch and log format errors
            System.err.println("Error parsing double from line: " + line);
            System.err.println("NumberFormatException: " + e.getMessage());
        }
    }
}