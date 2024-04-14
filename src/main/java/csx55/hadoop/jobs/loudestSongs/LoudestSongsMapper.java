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
        FileSplit split = (FileSplit) context.getInputSplit();
        identifier = split.getPath().getName();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        String songId = "";
        String outputValue = "";
        String artistID = "";

        if (identifier.contains("analysis")) {
            // Assuming 'analysis' file contains the SONG_ID_ANALYSIS and other fields
            songId = parts[Constants.Analysis.SONG_ID_ANALYSIS_INDEX];
            // Extracting loudness
            String loudness = parts[Constants.Analysis.LOUDNESS_INDEX];
            outputValue = "ANALYSIS_" + loudness;
        } else {
            // Assuming 'metadata' file contains the SONG_ID and other fields
            songId = parts[Constants.Metadata.SONG_ID_INDEX];
            // Extracting artistID and songTitle
            artistID = parts[Constants.Metadata.ARTIST_ID_INDEX];
            String artistName = parts[Constants.Metadata.ARTIST_NAME_INDEX];
            String songTitle = parts[Constants.Metadata.TITLE_INDEX];
            outputValue = "METADATA_" + "|" + artistName +"|" + songTitle;
        }

        context.write(new Text(artistID), new Text(outputValue));
    }
}

