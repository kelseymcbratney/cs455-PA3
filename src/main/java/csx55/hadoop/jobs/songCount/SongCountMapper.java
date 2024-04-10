package csx55.hadoop.jobs.songCount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SongCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Text artistName = new Text();
    private static final LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by tabs or other delimiters
        String[] fields = value.toString().split("\t");
        if (fields.length > 6) {
            // Extract the artist name
            String artist = fields[6];
            // Emit (artist_name, 1) pair
            artistName.set(artist);
            context.write(artistName, one);
        }
    }
}


