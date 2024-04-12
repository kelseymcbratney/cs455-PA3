package csx55.hadoop.jobs.loudestSongs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hadoop.Constants.Analysis.*;
import static csx55.hadoop.Constants.Metadata.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class LoudestSongsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String[]> artistMetadata = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (cacheFiles != null && cacheFiles.length > 0) {
            BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\|");
                if (fields.length >= csx55.hadoop.Constants.Metadata.ARTIST_ID_INDEX) {
                    // Assuming the artist ID is unique and is the key for metadata
                    artistMetadata.put(fields[csx55.hadoop.Constants.Metadata.ARTIST_ID_INDEX], fields);
                }
            }
            reader.close();
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");

        if (fields.length == 32) {
            String songId = fields[SONG_ID_ANALYSIS_INDEX];
            String loudness = fields[LOUDNESS_INDEX];
            context.write(new Text(songId), new Text("A," + loudness));
        } else if (fields.length == 14) {
            String artistId = fields[ARTIST_ID_INDEX];
            String[] metadata = artistMetadata.get(artistId);
            if (metadata != null) {
                String songName = metadata[TITLE_INDEX];
                String artistName = metadata[ARTIST_NAME_INDEX];
                context.write(new Text(artistId), new Text("B," + songName + "," + artistName));
            }
        }
    }
}
