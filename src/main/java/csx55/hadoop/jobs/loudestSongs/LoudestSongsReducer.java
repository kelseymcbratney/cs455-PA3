package csx55.hadoop.jobs.loudestSongs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LoudestSongsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String songName = "";
        String artistName = "";
        String loudness = "";
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts[0].equals("A")) {
                loudness = parts[1];
            } else if (parts[0].equals("B")) {
                songName = parts[1];
                artistName = parts[2];
            }
        }
        context.write(new Text(songName), new Text(artistName + "," + loudness));
    }
}
