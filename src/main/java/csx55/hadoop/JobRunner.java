package csx55.hadoop;

import csx55.hadoop.Constants;
import csx55.hadoop.jobs.songCount.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class JobRunner {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: JobRunner <inputDir1> <inputDir2> <outputDir> <jobType>");
            System.exit(1);
        }

        String jobType = args[3];

        switch (jobType) {
            case "1":
                runSongCountJob(args);
                runSortedSongCountJob(args);
                break;
            case "2":
                runSongCountJob(args);
                runSortedSongCountJob(args);
                break;
            default:
                System.err.println("Invalid job type. Available options:\n1. Run All Jobs, \n2. SongCount");
                System.exit(1);
        }
    }

    private static void runSongCountJob(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
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

    private static void runSortedSongCountJob(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JobRunner.class);

        job.setMapperClass(SongCountSortedMapper.class);
        job.setReducerClass(SongCountSortedReducer.class);

        job.setNumReduceTasks(1);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "_sorted"));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
