package csx55.hadoop.jobs.songCount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable result = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        // Sum up the counts for each artist
        for (LongWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        // Output the artist with the highest count
        context.write(key, result);
    }
}
