package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class LongestFadeSortedReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double originalFadeTime = -key.get();  // Revert the negation to get original fade time
        for (Text value : values) {
            context.write(value, new DoubleWritable(originalFadeTime));
        }
    }
}
