package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LoudestSongsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        double totalLoudness = 0;

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            if (fields.length == 3) {
                double loudness = Double.parseDouble(fields[1]);
                totalLoudness += loudness;
                count++;
            }
        }

        if (count > 0) {
            double averageLoudness = totalLoudness / count;
            context.write(new Text(key.toString()), new Text(Double.toString(averageLoudness)));
        }
    }
}
