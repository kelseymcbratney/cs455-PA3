package csx55.hadoop.jobs.longestSongs;

import csx55.hadoop.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LongestSongsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String identifier;

    @Override
    protected void setup(Context context) {
        FileSplit split = (FileSplit) context.getInputSplit();
        identifier = split.getPath().getName();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        String songId = "";
        String outputValue = "";

        if (identifier.contains("analysis")) {
            // Assuming 'analysis' file contains the SONG_ID_ANALYSIS and other fields
            songId = parts[Constants.Analysis.SONG_ID_ANALYSIS_INDEX];
            // Extracting loudness
            String duration = parts[Constants.Analysis.DURATION_INDEX];
            outputValue = "ANALYSIS_" + duration;
        } else {
            // Assuming 'metadata' file contains the SONG_ID and other fields
            songId = parts[Constants.Metadata.SONG_ID_INDEX];
            // Extracting artistID and songTitle
            String artistID = parts[Constants.Metadata.ARTIST_ID_INDEX];
            String artistName = parts[Constants.Metadata.ARTIST_NAME_INDEX];
            String songTitle = parts[Constants.Metadata.TITLE_INDEX];
            outputValue = "METADATA_" + artistID + "|" + artistName + "|" + songTitle;
        }

        context.write(new Text(songId), new Text(outputValue));
    }
}

