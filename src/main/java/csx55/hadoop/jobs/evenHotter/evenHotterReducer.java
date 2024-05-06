package csx55.hadoop.jobs.evenHotter;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
                String[] parts = val.toString().substring(9).split("\\^");
                songTitle = parts[0];
                hotttnesss = val.toString().substring(9);
                // outputValue = "ANALYSIS_" + hotttnesss + " ^ " + tempo + " ^ " + timeSignature + " ^ " + keySignature + " ^ " + energy + " ^ " + danceability + " ^ " + duration + " ^ " + loudness;
                tempo = parts[1];
                timeSignature = parts[2];
                keySignature = parts[3];
                energy = parts[4];
                danceability = parts[5];
                duration = parts[6];
                loudness = parts[7];

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
