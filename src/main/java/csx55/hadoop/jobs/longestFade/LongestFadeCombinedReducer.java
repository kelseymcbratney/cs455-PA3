package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class LongestFadeCombinedReducer extends Reducer<Text, Text, DoubleWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalFadeTime = 0;
        String artistName = "";
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            double fadeTime = Double.parseDouble(parts[0].trim());
            artistName = parts[1].trim();  // Assuming last part after the split is the artist name
            totalFadeTime += fadeTime;
            count++;
        }

        if (count > 0) {
            double averageFadeTime = totalFadeTime / count;
            context.write(new DoubleWritable(-averageFadeTime), new Text(key.toString() + ", " + artistName));
        }
    }
}