package csx55.hadoop.jobs.preProccessing;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PreProcessing {

    public static class PreProcessingMapper extends Mapper<Object, Text, Text, Text> {

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Assuming first file contains keys and second file contains values
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String keyStr = tokenizer.nextToken();
            String valueStr = tokenizer.nextToken();
            keyOut.set(keyStr);
            valueOut.set(valueStr);
            context.write(keyOut, valueOut);
        }
    }

    public static class PreProcessingReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            for (Text val : values) {
                stringBuilder.append(val.toString()).append(",");
            }
            result.set(stringBuilder.toString());
            context.write(key, result);
        }
    }
}