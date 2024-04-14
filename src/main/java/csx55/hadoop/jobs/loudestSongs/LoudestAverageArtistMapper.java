package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LoudestAverageArtistMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input Text value into parts based on the comma followed by a space
        String[] parts = value.toString().split(", ");
        // Ensure there are enough parts to process without index out of bounds error
        if (parts.length >= 4) {
            try {
                String trackId = parts[0];  // Assuming first part is Track ID (not used in context.write())
                String artistId = parts[1]; // Assuming second part is Artist ID
                String artistName = parts[2]; // Assuming third part is Artist Name
                String loudness = parts[3]; // Assuming fourth part is Loudness
                // Write the artist ID as key, and artist name and loudness as value
                context.write(new Text(artistId), new Text(artistName + ", " + loudness));
            } catch (NumberFormatException e) {
                System.err.println("NumberFormatException: " + e.getMessage());
            }
        } else {
            System.err.println("Invalid data format: " + value.toString());
        }
    }
}
