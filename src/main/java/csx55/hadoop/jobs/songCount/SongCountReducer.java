package csx55.hadoop.jobs.songCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SongCountReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        // Iterate through all the values for a particular key
        String[] parts = null;
        int artistIndex = 0;
        int countIndex = 0;
        for (Text value : values) {
            parts = value.toString().split(",");
            // Assuming the count is at the last index
            countIndex = parts.length - 1;
            artistIndex = parts.length - 3;
            sum += Long.parseLong(parts[countIndex]);
        }
        // Write the sum as the key and the original key as the value
        context.write(key,new Text(parts[artistIndex] + "," + sum));
    }
}