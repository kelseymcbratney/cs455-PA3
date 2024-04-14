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
        String artistName = "";

        for (Text val : values) {
            String data = val.toString();
            try {
                if (data.startsWith("ANALYSIS_")) {
                    // Extract loudness, assuming it follows the tag directly
                    loudness = data.substring(9);
                } else if (data.startsWith("METADATA_")) {
                    // Check if data is correctly formatted
                    String[] parts = data.substring(9).split("\\|");
                    if (parts.length >= 1) { // Ensure there are at least two parts: artistID and songTitle
                        artistName = parts[0];
                        songTitle = parts[1];
                    } else {
                        // Log or handle incomplete metadata records
                        System.err.println("Incomplete METADATA record for key " + key.toString() + ": " + data);
                    }
                }
            } catch (Exception e) {
                // Log the exception with context
                System.err.println("Error processing record for key " + key.toString() + ": " + data);
                e.printStackTrace();
            }
        }

        // Only output if all parts are non-empty and valid
        if (!artistID.isEmpty() && !songTitle.isEmpty() && !loudness.isEmpty()) {
            context.write(key, new Text(artistName + ", " + songTitle + ", " + loudness));
        }
    }
}
