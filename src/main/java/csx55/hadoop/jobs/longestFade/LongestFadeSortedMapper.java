package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LongestFadeSortedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int firstSpaceIndex = line.indexOf(" ");  // Assuming the first space separates the fade time from the rest
        double fadeTime = Double.parseDouble(line.substring(0, firstSpaceIndex));
        String artistDetails = line.substring(firstSpaceIndex + 1);

        context.write(new DoubleWritable(-fadeTime), new Text(artistDetails));  // Negate to sort in descending order
    }
}
