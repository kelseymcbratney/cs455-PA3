package csx55.hadoop.jobs.mostEngergetic;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class MostEnergeticReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistID = "";
        String songTitle = "";
        String dancability = "";
        String artistName = "";
        String energy = "";

        for (Text val : values) {
            String data = val.toString();
            try {
                if (data.startsWith("ANALYSIS_")) {
                    String[] parts = data.substring(9).split("\\|");
                    if (parts.length >= 2) {
                        dancability = parts[0];
                        energy = parts[1];
                    } else {
                        System.err.println("Incomplete ANALYSIS record for key " + key.toString() + ": " + data);
                    }
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
        if (!artistID.isEmpty() && !songTitle.isEmpty() && !dancability.isEmpty() && !energy.isEmpty()) {
            context.write(key, new Text(artistID + ", " + artistName + ", " + songTitle + ", " + dancability + ", " + energy));
        }
    }
}
