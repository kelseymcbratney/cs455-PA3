package csx55.hadoop;

import csx55.hadoop.jobs.preProccessing.PreProcessing;

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
        job.setJobName("CombineFiles");

        job.setMapperClass(PreProcessing.PreProcessingMapper.class);
        job.setReducerClass(PreProcessing.PreProcessingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
