package csx55.hadoop.jobs.songCount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongCountSortedReducer extends Reducer<Text, Text, LongWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        // Iterate through all the values for a particular key
        for (Text value : values) {
            sum += Long.parseLong(value.toString());
        }
        // Write the sum as the key and the original key as the value
        context.write(new LongWritable(sum), key);
    }
}