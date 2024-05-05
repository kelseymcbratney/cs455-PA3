package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class LongestFadeCombinedMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length >= 6) {
            String artistParts = parts[0].trim();
            String[] artistPartsSplit = artistParts.split("\t");
            String artistID = artistPartsSplit[0].trim();
            String artistName = parts[1].trim();
            double endOfFadeIn = Double.parseDouble(parts[3].trim());
            double duration = Double.parseDouble(parts[4].trim());
            double startOfFadeOut = Double.parseDouble(parts[5].trim());

            // Calculate fade time: end of fade in + (duration - start of fade out)
            double fadeTime = endOfFadeIn + duration - startOfFadeOut;
            context.write(new Text(artistID), new Text(fadeTime + ", " + artistName));
        }
    }
}