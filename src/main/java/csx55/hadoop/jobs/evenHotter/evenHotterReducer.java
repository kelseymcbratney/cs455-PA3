package csx55.hadoop.jobs.evenHotter;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class evenHotterReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistName = "";
        String songTitle = "";
        String hotttnesss = "";
        String tempo = "";
        String timeSignature = "";
        String keySignature = "";
        String energy = "";
        String danceability = "";
        String duration = "";
        String loudness = "";

        for (Text val : values) {
            if (val.toString().startsWith("ANALYSIS_")) {
                // mapper output = "ANALYSIS_" + hotttnesss + "|" + tempo + "|" + timeSignature + "|" + keySignature + "|" + energy + "|" + danceability + "|" + duration + "|" + loudness;
                String[] parts = val.toString().substring(9).split("\\|");
                songTitle = parts[0];
                String[] analysisParts = val.toString().substring(9).split("\\|");
                hotttnesss = analysisParts[0];
                tempo = analysisParts[1];
                timeSignature = analysisParts[2];
                keySignature = analysisParts[3];
                energy = analysisParts[4];
                danceability = analysisParts[5];
                duration = analysisParts[6];
                loudness = analysisParts[7];


            } else if (val.toString().startsWith("METADATA_")) {
                String[] metaParts = val.toString().substring(9).split("\\|");
                artistName = metaParts[0];
            }
        }

        if (!hotttnesss.isEmpty() && !artistName.isEmpty() && !songTitle.isEmpty()) {
            context.write(key, new Text(artistName + "|" + songTitle + "|" + hotttnesss + "|" + tempo + "|" + timeSignature + "|" + keySignature + "|" + energy + "|" + danceability + "|" + duration + "|" + loudness));
        }
    }
}
