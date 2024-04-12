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
        String type = null;
        String data = null;

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            type = parts[0];
            data = parts[1];

//            if (type.equals("A")) {
//                if (!data.equals("nan")) { // Filter out NaN values
//                    loudness = data;
//                }
//            } else if (type.equals("B")) {
//                int commaIndex = data.indexOf(',');
//                if (commaIndex != -1) {
//                    songName = data.substring(0, commaIndex);
//                    artistName = data.substring(commaIndex + 1);
//                } else {
//                    songName = data; // Assuming entire data is songName if no comma is present
//                    artistName = "Unknown"; // Default or null, depending on requirement
//                }
//            }

        }

        context.write(key, new Text(type + " - " + data));
    }
}
