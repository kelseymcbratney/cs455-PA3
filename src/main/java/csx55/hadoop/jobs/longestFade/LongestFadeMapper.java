package csx55.hadoop.jobs.longestFade;

import csx55.hadoop.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LongestFadeMapper extends Mapper<LongWritable, Text, Text, Text> {
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
        String duration = "";
        String startOfFadeOut = "";
        String fadeDuration = "";

        if (identifier.contains("analysis")) {
            // Assuming 'analysis' file contains the SONG_ID_ANALYSIS and other fields
            songId = parts[Constants.Analysis.SONG_ID_ANALYSIS_INDEX];
            // Extracting loudness
            String endOfFadein = parts[Constants.Analysis.END_OF_FADE_IN_INDEX];
            duration = parts[Constants.Analysis.DURATION_INDEX];
            startOfFadeOut = parts[Constants.Analysis.START_OF_FADE_OUT_INDEX];
            outputValue = "ANALYSIS_" + endOfFadein + "|" + duration + "|" + startOfFadeOut;
        } else {
            // Assuming 'metadata' file contains the SONG_ID and other fields
            songId = parts[Constants.Metadata.SONG_ID_INDEX];
            // Extracting artistID and songTitle
            String artistID = parts[Constants.Metadata.ARTIST_ID_INDEX];
            String artistName = parts[Constants.Metadata.ARTIST_NAME_INDEX];
            outputValue = "METADATA_" + artistID + "|" + artistName;
        }

        context.write(new Text(songId), new Text(outputValue));
    }
}

