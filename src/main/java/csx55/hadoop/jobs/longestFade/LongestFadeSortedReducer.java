package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class LongestFadeSortedReducer extends Reducer<Text, Text, DoubleWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalFadeTime = 0;
        int count = 0;

        for (Text val : values) {
            totalFadeTime += Double.parseDouble(val.toString());
            count++;
        }

        if (count > 0) {
            double averageFadeTime = totalFadeTime / count;
            // Emit negative to sort in descending order
            context.write(new DoubleWritable(-averageFadeTime), key);
        }
    }
}
