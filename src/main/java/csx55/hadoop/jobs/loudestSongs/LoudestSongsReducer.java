package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestSongsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, IOException {
        String metadata = null;
        String analysis = null;

        for (Text val : values) {
            if (val.toString().startsWith("ANALYSIS_")) {
                analysis = val.toString().substring(9);
            } else if (val.toString().startsWith("METADATA_")) {
                metadata = val.toString().substring(9);
            }
        }

        if (analysis != null && metadata != null) {
            context.write(key, new Text(metadata + "|" + analysis));
        }
    }
}
