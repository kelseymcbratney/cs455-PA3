package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hadoop.Constants.Analysis.*;
import static csx55.hadoop.Constants.Metadata.*;

public class LoudestSongsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int SONG_ID_ANALYSIS_INDEX = 0;
    private static final int LOUDNESS_INDEX = 1;
    private static final int TITLE_INDEX = 0;
    private static final int ARTIST_NAME_INDEX = 1;
    private static final int ARTIST_ID_INDEX = 2;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into parts
        String[] fields = value.toString().split("\\|");

        if (fields.length == 32) {
            // Extract the song id and loudness from the first input type
            String songId = fields[SONG_ID_ANALYSIS_INDEX];
            String loudness = fields[LOUDNESS_INDEX];
            context.write(new Text(songId), new Text("A|" + loudness));
        } else if (fields.length == 14) {
            // Extract the song name, artist name, and artist id from the second input type
            String songName = fields[TITLE_INDEX];
            String artistName = fields[ARTIST_NAME_INDEX];
            String artistId = fields[ARTIST_ID_INDEX];
            context.write(new Text(artistId), new Text("B|" + songName + "," + artistName));
        }
    }
}
