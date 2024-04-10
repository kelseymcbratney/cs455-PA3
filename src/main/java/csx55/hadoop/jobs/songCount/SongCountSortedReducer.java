package csx55.hadoop.jobs.songCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongCountSortedReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException, IOException {
        for (Text value : values) {
            // Emit the artist and count. Since the key is the count, it's already sorted.
            context.write(value, key);
        }
    }
}
