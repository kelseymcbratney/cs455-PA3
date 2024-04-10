package csx55.hadoop.jobs.songCount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static csx55.hadoop.Constants.Analysis.*;
import static csx55.hadoop.Constants.Metadata.*;
import static csx55.hadoop.Constants.Analysis.*;

public class SongCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Text artistName = new Text();
    private static final LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by tabs or other delimiters
        String[] fields = value.toString().split("\\|");
        if (fields.length == 13) {
            // Extract the artist name
            String artist_id = fields[ARTIST_ID_INDEX];
            String artist_name = fields[ARTIST_NAME_INDEX];
            String song_id = fields[SONG_ID_INDEX];
            String song_name = fields[TITLE_INDEX];

            context.write(new Text(artist_id), new Text("Metadata" + "," + artist_name + "," + song_id + "," + song_name + "," + "1"));
        }
    }
}


