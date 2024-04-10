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

            // Specify the input and output paths
            // Ensure the input path is the output path of your first MapReduce job
            FileInputFormat.addInputPath(sortJob, new Path(args[2])); // Assuming args[1] is the correct input path
            FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_sorted")); // Adjust if necessary

            // Execute the job and wait for it to finish
            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
        System.exit(success ? 0 : 1);
    }
}
