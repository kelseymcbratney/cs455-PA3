package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;

public class LongestFadeFinalReducer extends Reducer<Text, Text, DoubleWritable, Text> {
    private HashMap<String, Double> artistFadeTimes = new HashMap<>();
    private String recordFadeArtist = "";
    private double recordFadeTime = 0;
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalFadeTime = 0;
        String artistName = "";

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            totalFadeTime = Double.parseDouble(parts[0].trim());
            artistName = parts[1].trim();
        }

        if (artistName != null && !artistName.isEmpty()) {
            totalFadeTime = artistFadeTimes.getOrDefault(artistName, 0.0) + totalFadeTime;
            artistFadeTimes.put(artistName, totalFadeTime);

            if (totalFadeTime > recordFadeTime){
                recordFadeTime = totalFadeTime;
                recordFadeArtist = artistName;

            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        context.write(new DoubleWritable(recordFadeTime), new Text(recordFadeArtist));

    }

}