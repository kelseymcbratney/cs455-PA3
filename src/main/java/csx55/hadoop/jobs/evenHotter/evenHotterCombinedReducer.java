package csx55.hadoop.jobs.evenHotter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class evenHotterCombinedReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double hotttnesss = -key.get(); // Convert back to positive as we negated it in mapper
        for (Text value : values) {
            String[] parts = value.toString().split("\\|");
            if (parts.length == 9) {  // artistName and songTitle
                String output = parts[0] + " - " + parts[1] + " - " + parts[2] + " - " + parts[3] + " - " + parts[4] + " - " + parts[5] + " - " + parts[6] + " - " + parts[7] + " - " + parts[8];
                context.write(new Text(output), new DoubleWritable(hotttnesss));
            }
        }
    }
}
