package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class LongestFadeSortedReducer extends Reducer<Text, Text, DoubleWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalFadeTime = 0;
        String artistName = "";
        int count = 0;

        for (Text val : values) {
            //split val to get artistName and FaceTime
            String[] parts = val.toString().split(",");
            //add FaceTime to totalFadeTime
            artistName = parts[1];
            totalFadeTime += Double.parseDouble(parts[0]);
            count++;
        }

        if (count > 0) {
            double averageFadeTime = totalFadeTime / count;
            key = new Text(key.toString().split("\t")[0]);
            context.write(new DoubleWritable(-averageFadeTime), new Text(key + ", " + artistName));
        }
    }
}