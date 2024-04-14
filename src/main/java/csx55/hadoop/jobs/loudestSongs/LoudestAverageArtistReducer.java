package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LoudestAverageArtistReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        for (Text val : values) {
            sum += Double.parseDouble(val.toString());
            count++;
        }
        double average = count > 0 ? sum / count : 0;
        context.write(key, new Text(String.valueOf(average)));
    }
}