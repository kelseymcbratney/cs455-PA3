package csx55.hadoop.jobs.hotttnesss;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class hotttnesssSongReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistName = "";
        String songTitle = "";
        String hotttnesss = "";

        for (Text val : values) {
            if (val.toString().startsWith("ANALYSIS_")) {
                String[] parts = val.toString().substring(9).split("\\|");
                songTitle = parts[0];
                hotttnesss = val.toString().substring(9);
            } else if (val.toString().startsWith("METADATA_")) {
                String[] metaParts = val.toString().substring(9).split("\\|");
                artistName = metaParts[0];
            }
        }

        if (!hotttnesss.isEmpty() && !artistName.isEmpty() && !songTitle.isEmpty()) {
            context.write(key, new Text(artistName + "|" + songTitle + "|" + hotttnesss));
        }
    }
}
