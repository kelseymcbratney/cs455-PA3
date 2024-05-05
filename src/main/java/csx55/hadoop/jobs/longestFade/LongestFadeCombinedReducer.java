package csx55.hadoop.jobs.longestFade;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;

public class LongestFadeCombinedReducer extends Reducer<Text, Text, DoubleWritable, Text> {
    private HashMap<String, Double> artistFadeTimes = new HashMap<>();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalFadeTime = 0;
        double recordFadeTime = 0;
        String artistName = "";
        String recordFadeArtist = "";
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            totalFadeTime = Double.parseDouble(parts[0].trim());
            artistName = parts[1].trim();  // Assuming last part after the split is the artist name
        }

        if (artistName != null && !artistName.isEmpty()) {
            artistFadeTimes.put(artistName, totalFadeTime);

            if (totalFadeTime > recordFadeTime){
                recordFadeTime = totalFadeTime;
                recordFadeArtist = artistName;

            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String artist : artistFadeTimes.keySet()) {
            context.write(new DoubleWritable(artistFadeTimes.get(artist)), new Text(artist));
        }
    }
}