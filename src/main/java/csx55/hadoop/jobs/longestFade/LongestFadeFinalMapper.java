package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LongestFadeFinalMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length >= 2) {
            String artistParts = parts[0].trim();
            String[] artistPartsSplit = artistParts.split("\t");
            String artistID = artistPartsSplit[0].trim();
            String artistName = parts[1].trim();
            double fadeTime = Double.parseDouble(artistPartsSplit[1].trim());
            context.write(new Text(artistID), new Text(fadeTime + ", " + artistName));
    }
}
}