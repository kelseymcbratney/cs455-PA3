package csx55.hadoop.jobs.loudestSongs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LoudestSongsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        String songName = null;
//        String loudness = null;
//        String artistName = null;
//
//        for (Text value : values) {
//            String[] parts = value.toString().split("\\|");
//            String type = parts[0];
//            String data = parts[1];
//
//            if (type.equals("A")) {
//                loudness = data;
//            } else if (type.equals("B")) {
//                String[] songInfo = data.split(",");
//                songName = songInfo[0];
//                artistName = songInfo[1];
//            }
//        }
//
//        if (songName != null && loudness != null) {
//            context.write(key, new Text(songName + "," + loudness + "," + artistName));
//        }
        // output key value as is
        for (Text value : values) {
            context.write(key, value);
        }
    }
}