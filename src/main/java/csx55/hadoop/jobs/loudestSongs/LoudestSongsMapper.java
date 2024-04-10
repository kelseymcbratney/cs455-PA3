package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hadoop.Constants.Analysis.*;
import static csx55.hadoop.Constants.Metadata.*;


public class LoudestSongsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // Split the input line into parts
        String[] fields = value.toString().split("\\|");
        String song_id = null;
        String song_name = null;
        String loudness = null;
        String artist_name = null;
        String artist_id = null;

        if (fields.length == 32) {
            // Extract the song name and loudness
            song_id = fields[SONG_ID_ANALYSIS_INDEX];
            loudness = fields[LOUDNESS_INDEX];

        } else if (fields.length == 14) {
            // Extract the song name and loudness
            song_name = fields[TITLE_INDEX];
            artist_name = fields[ARTIST_NAME_INDEX];
            artist_id = fields[ARTIST_ID_INDEX];
        }

        context.write(new Text(song_id), new Text(song_name + "," + loudness + "," + artist_name + "," + artist_id));
    }
}
