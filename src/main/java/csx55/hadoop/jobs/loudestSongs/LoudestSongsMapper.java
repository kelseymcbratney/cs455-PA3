package csx55.hadoop.jobs.loudestSongs;

import csx55.hadoop.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LoudestSongsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String identifier;

    @Override
    protected void setup(Context context) {
        // Determine which file the mapper is currently processing
        FileSplit split = (FileSplit) context.getInputSplit();
        identifier = split.getPath().getName();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");

        // Initialize songId and tag with filename to differentiate data in the reducer
        String songId = "";
        String tag = identifier.contains("analysis.txt") ? "ANALYSIS_" : "METADATA_";

        if (identifier.contains("analysis.txt")) {
            // Assuming 'analysis' file contains the SONG_ID_ANALYSIS
            songId = parts[Constants.Analysis.SONG_ID_ANALYSIS_INDEX];
        } else {
            // Assuming 'metadata' file contains the SONG_ID
            songId = parts[Constants.Metadata.SONG_ID_INDEX];
        }

        // Output the song ID as key, and append a tag to the value to identify the source file
        context.write(new Text(songId), new Text(tag + value.toString()));
    }
}
