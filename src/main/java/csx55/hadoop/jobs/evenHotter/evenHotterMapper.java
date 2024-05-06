package csx55.hadoop.jobs.evenHotter;

import csx55.hadoop.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class evenHotterMapper extends Mapper<LongWritable, Text, Text, Text> {
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
            String hotttnesss = parts[Constants.Analysis.SONG_HOTTNESSS_INDEX];
            String tempo = parts[Constants.Analysis.TEMPO_INDEX];
            String timeSignature = parts[Constants.Analysis.TIME_SIGNATURE_INDEX];
            String keySignature = parts[Constants.Analysis.KEY_INDEX];
            String energy = parts[Constants.Analysis.ENERGY_INDEX];
            String danceability = parts[Constants.Analysis.DANCEABILITY_INDEX];
            String duration = parts[Constants.Analysis.DURATION_INDEX];
            String loudness = parts[Constants.Analysis.LOUDNESS_INDEX];

            outputValue = "ANALYSIS_" + hotttnesss + "|" + tempo + "|" + timeSignature + "|" + keySignature + "|" + energy + "|" + danceability + "|" + duration + "|" + loudness;
        } else {
            // Assuming 'metadata' file contains the SONG_ID and other fields
            songId = parts[Constants.Metadata.SONG_ID_INDEX];
            // Extracting artistID and songTitle
            String songTitle = parts[Constants.Metadata.TITLE_INDEX];
            outputValue = "METADATA_" + songTitle;
        }

        context.write(new Text(songId), new Text(outputValue));
    }
}

