package csx55.hadoop;

import csx55.hadoop.Constants;
import csx55.hadoop.jobs.songCount.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRunner {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: JobRunner <inputDir1> <inputDir2> <outputDir>");
            System.exit(1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(JobRunner.class);
        job.setJobName("SongCount");

        job.setMapperClass(SongCountMapper.class);
        job.setReducerClass(SongCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
