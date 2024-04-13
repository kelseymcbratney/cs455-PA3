package csx55.hadoop;

import csx55.hadoop.jobs.loudestSongs.*;
import csx55.hadoop.jobs.songCount.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class JobRunner {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: JobRunner <inputDir1> <inputDir2> <outputDir> <jobType> Available options:\n1. Run All Jobs, \n2. SongCount");
            System.exit(1);
        }

        String jobType = args[3];

        switch (jobType) {
            case "0": // Run all jobs
                if (!runSongCountJob(args)) {
                    System.exit(1);
                }
                if (!runLoudestSongsJob(args)) {
                    System.exit(1);
                }
                break;
            case "1":
                if (!runSongCountJob(args)) {
                    System.exit(1);
                }
                break;
            case "2":
                if (!runLoudestSongsJob(args)) {
                    System.exit(1);
                }
                break;
            default:
                System.err.println("Invalid job type. Available options:\n1. Run All Jobs, \n2. SongCount");
                System.exit(1);
        }
    }

    private static boolean runSongCountJob(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new Path(args[1]).toUri(), conf);

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

        if (success) {
            Job sortJob = Job.getInstance();
            sortJob.setJarByClass(JobRunner.class); // Ensure JobRunner is your main class

            // Set the new classes for the mapper and reducer
            sortJob.setMapperClass(SongCountSortedMapper.class);
            sortJob.setReducerClass(SongCountSortedReducer.class);

            // Setting the number of reduce tasks
            sortJob.setNumReduceTasks(1); // Only one reducer to ensure global ordering

            // If you want to sort in descending order, use the DecreasingComparator as before
            sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            // Set the map output key/value classes according to the mapper's outputs
            sortJob.setMapOutputKeyClass(LongWritable.class);
            sortJob.setMapOutputValueClass(Text.class);

            // Set the final output key/value classes according to the reducer's outputs
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(sortJob, new Path(args[2]));
            FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_songCount"));

            // Execute the sort job and wait for it to finish
            success = sortJob.waitForCompletion(true);
        }

        return success;
    }


    private static boolean runLoudestSongsJob(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JobRunner.class);
        job.setJobName("LoudestSongs");

        job.setMapperClass(LoudestSongsMapper.class);
        job.setReducerClass(LoudestSongsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "_loudestSongs"));

        boolean success = job.waitForCompletion(true);

        if (success) {
            Job sortJob = Job.getInstance();
            sortJob.setJarByClass(JobRunner.class); // Ensure JobRunner is your main class

            // Set the new classes for the mapper and reducer
            sortJob.setMapperClass(LoudestAverageArtistMapper.class);
            sortJob.setReducerClass(LoudestAverageArtistReducer.class);

            // Set the final output key/value classes according to the reducer's outputs
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(sortJob, new Path(args[2] + "_loudestSongs"));
            FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_loudestAverageArtist"));

            // Execute the sort job and wait for it to finish
            success = sortJob.waitForCompletion(true);
        }

        return success;

    }
}
