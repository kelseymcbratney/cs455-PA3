package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestSongsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistID = "";
        String songTitle = "";
        String loudness = "";

        for (Text val : values) {
            if (val.toString().startsWith("ANALYSIS_")) {
                loudness = val.toString().substring(9);
            } else if (val.toString().startsWith("METADATA_")) {
                String[] parts = val.toString().substring(9).split("\\|");
                artistID = parts[0];
                songTitle = parts[1];
            }
        }

        if (!artistID.isEmpty() && !songTitle.isEmpty() && !loudness.isEmpty()) {
            context.write(key, new Text("(" + artistID + ", " + songTitle + ", " + loudness + ")"));
        }
    }
}
