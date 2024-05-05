package csx55.hadoop.jobs.mostEngergetic;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostEngergeticCombinedReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException, IOException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
