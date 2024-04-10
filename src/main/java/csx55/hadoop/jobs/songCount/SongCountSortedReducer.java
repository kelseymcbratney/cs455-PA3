package csx55.hadoop.jobs.songCount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongCountSortedReducer extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            long count = Long.parseLong(parts[0]);
            context.write(new LongWritable(count), new Text(parts[1]));
        }
    }
}