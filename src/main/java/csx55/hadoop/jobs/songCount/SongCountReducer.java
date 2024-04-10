package csx55.hadoop.jobs.songCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SongCountReducer extends Reducer<Text, Text, LongWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        // Iterate through all the values for a particular key
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            // Assuming the count is at the last index
            int countIndex = parts.length - 1;
            sum += Long.parseLong(parts[countIndex]);
        }
        // Write the sum as the key and the original key as the value
        context.write(new LongWritable(sum), key);
    }
}