package csx55.hadoop.jobs.loudestSongs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LoudestSongsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String songName = null;
        String loudness = null;
        String artistName = null;

        for (Text value : values) {
            String[] parts = value.toString().split(",", 3); // Split into at most three parts
            String type = parts[0];
            String data1 = parts[1];

            if (type.equals("A")) {
                loudness = data1; // Direct assignment of loudness
            } else if (type.equals("B")) {
                songName = data1;
                if (parts.length > 2) { // Check if artist name is available
                    artistName = parts[2];
                } else {
                    artistName = "Unknown"; // Handle missing artist names gracefully
                }
            }
        }

        // Only write out to context if we have both a song name and loudness
        if (songName != null && loudness != null) {
            context.write(key, new Text(songName + "," + loudness + "," + artistName));
        }
    }
}
