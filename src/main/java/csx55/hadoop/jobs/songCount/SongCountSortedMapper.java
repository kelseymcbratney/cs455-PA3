package csx55.hadoop.jobs.songCount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongCountSortedMapper extends Mapper<Object, Text, LongWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        try {
            long count = Long.parseLong(parts[parts.length - 1].trim());
            String artistName = parts[parts.length - 3].trim();
            context.write(new LongWritable(count), new Text(artistName));
        } catch (NumberFormatException e) {
            System.err.println("Error parsing count in SongCountSortedMapper: " + e.getMessage());
        }
    }
}
