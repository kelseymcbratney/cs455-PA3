package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class LongestFadeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistID = "";
        String songTitle = "";
        String fadein = "";
        String artistName = "";

        for (Text val : values) {
            String data = val.toString();
            try {
                if (data.startsWith("ANALYSIS_")) {
                    // Extract loudness, assuming it follows the tag directly
                    fadein = data.substring(9);
                } else if (data.startsWith("METADATA_")) {
                    // Check if data is correctly formatted
                    String[] parts = data.substring(9).split("\\|");
                    if (parts.length >= 2) { // Ensure there are at least two parts: artistID and songTitle
                        artistID = parts[0];
                        artistName = parts[1];
                        songTitle = parts[2];
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
        if (!artistID.isEmpty() && !songTitle.isEmpty() && !fadein.isEmpty()) {
            context.write(key, new Text(artistID + ", " + artistName + ", " + songTitle + ", " + fadein));
        }
    }
}
