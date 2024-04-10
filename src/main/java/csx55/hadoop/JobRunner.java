package csx55.hadoop;

import csx55.hadoop.Constants;
import csx55.hadoop.jobs.songCount.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            // Run the second job for sorting
            Job sortJob = Job.getInstance();

            sortJob.setJarByClass(JobRunner.class);
            sortJob.setMapperClass(SongCountSortedReducer.class);
            sortJob.setNumReduceTasks(1); // Only one reducer for sorting
            sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            sortJob.setMapOutputKeyClass(LongWritable.class);
            sortJob.setMapOutputValueClass(Text.class);

            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(sortJob, new Path(args[1]));
            FileOutputFormat.setOutputPath(sortJob, new Path(args[1] + "_sorted"));

            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }

        System.exit(success ? 0 : 1);
    }
}
