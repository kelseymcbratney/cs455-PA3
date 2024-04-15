package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LongestFadeSortedReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double fadeTime = -key.get(); // Convert back to positive as we negated it in mapper
        for (Text value : values) {
            context.write(value, new DoubleWritable(fadeTime));
        }
    }
}
