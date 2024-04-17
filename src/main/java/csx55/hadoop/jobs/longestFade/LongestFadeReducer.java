package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class LongestFadeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistID = "";
        String songTitle = "";
        String endOfFadein = "";
        String artistName = "";
        String duration = "";
        String startOfFadeOut = "";

        for (Text val : values) {
            String data = val.toString();
            try {
                if (data.startsWith("ANALYSIS_")) {
                    String parts[] = data.substring(9).split("\\|");
                    if (parts.length >= 3) {
                        endOfFadein = parts[0];
                        duration = parts[1];
                        startOfFadeOut = parts[2];
                    } else {
                        System.err.println("Incomplete ANALYSIS record for key " + key.toString() + ": " + data);
                    }
                } else if (data.startsWith("METADATA_")) {
                    // Check if data is correctly formatted
                    String[] parts = data.substring(9).split("\\|");
                    if (parts.length >= 2) { // Ensure there are at least two parts: artistID and songTitle
                        artistID = parts[0];
                        artistName = parts[1];
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
        if (!artistID.isEmpty() && !artistName.isEmpty() && !endOfFadein.isEmpty() && !duration.isEmpty() && !startOfFadeOut.isEmpty()){
            context.write(key, new Text(artistID + ", " + artistName + ", " + songTitle + ", " + endOfFadein + ", " + duration + ", " + startOfFadeOut));
        }
    }
}
