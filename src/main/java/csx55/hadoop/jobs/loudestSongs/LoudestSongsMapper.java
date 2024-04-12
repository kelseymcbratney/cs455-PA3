package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hadoop.Constants.Analysis.*;
import static csx55.hadoop.Constants.Metadata.*;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class LoudestSongsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, List<String>> songDataCache = new HashMap<>();
    private Map<String, String> artistDataCache = new HashMap<>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");

        if (fields.length == 32) {
            // Analysis data case
            String songId = fields[SONG_ID_ANALYSIS_INDEX];
            String loudness = fields[LOUDNESS_INDEX];
            String artistId = fields[ARTIST_ID_INDEX]; // Assuming artist ID is present in the analysis data

            if (artistDataCache.containsKey(artistId)) {
                String artistData = artistDataCache.get(artistId);
                String[] artistFields = artistData.split(",");
                String songName = artistFields[1];  // Assuming the song name is stored at index 1
                context.write(new Text(artistId), new Text("(" + songId + ", " + songName + ", " + loudness + ")"));
            } else {
                // Cache the song data as it lacks artist info
                songDataCache.computeIfAbsent(artistId, k -> new ArrayList<>()).add(songId + "," + loudness);
            }
        } else if (fields.length == 14) {
            // Metadata case
            String artistId = fields[ARTIST_ID_INDEX];
            String songName = fields[TITLE_INDEX];
            artistDataCache.put(artistId, songName);

            if (songDataCache.containsKey(artistId)) {
                for (String songDetail : songDataCache.get(artistId)) {
                    String[] songFields = songDetail.split(",");
                    String songId = songFields[0];
                    String loudness = songFields[1];
                    context.write(new Text(artistId), new Text("(" + songId + ", " + songName + ", " + loudness + ")"));
                }
                songDataCache.remove(artistId);
            }
        }
    }
}
