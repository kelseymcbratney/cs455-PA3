package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //context.write(new Text(artistID), new Text(artistName + ", " + loudness));
        String artistName = "";
        String artistID = "";
        String songTitle = "";
        double sum = 0;
        int count = 0;
        for (Text val : values) {
            String[] parts = val.toString().split(", ");
            sum += Double.parseDouble(parts[3]);
            count++;
            artistName = parts[1];
        }
        double average = count > 0 ? sum / count : 0;
        context.write(key, new Text(artistName + average));
    }
}
