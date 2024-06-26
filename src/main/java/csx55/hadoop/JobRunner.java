package csx55.hadoop;

import csx55.hadoop.jobs.evenHotter.evenHotterCombinedMapper;
import csx55.hadoop.jobs.evenHotter.evenHotterCombinedReducer;
import csx55.hadoop.jobs.evenHotter.evenHotterMapper;
import csx55.hadoop.jobs.evenHotter.evenHotterReducer;
import csx55.hadoop.jobs.hotttnesss.*;
import csx55.hadoop.jobs.longestFade.*;
import csx55.hadoop.jobs.longestSongs.*;
import csx55.hadoop.jobs.loudestSongs.*;
import csx55.hadoop.jobs.mostEngergetic.*;
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
                if (!runTopHotttnesss(args)) {
                    System.exit(1);
                }
                if (!runTopFadeIn(args)) {
                    System.exit(1);
                }
                if (!runSongLongestSong(args)) {
                    System.exit(1);
                }
                if (!runMostEnergetic(args)) {
                    System.exit(1);
                }
                if (!runEvenHotter(args)) {
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
            case "3":
                if (!runTopHotttnesss(args)) {
                    System.exit(1);
                }
                break;
            case "4":
                if (!runTopFadeIn(args)) {
                    System.exit(1);
                }
                break;
            case "5":
                if (!runSongLongestSong(args)) {
                    System.exit(1);
                }
                break;
            case "6":
                if (!runMostEnergetic(args)) {
                    System.exit(1);
                }
                break;
            case "7":
                if (!runEvenHotter(args)) {
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
            sortJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(sortJob, new Path(args[2] + "_loudestSongs"));
            FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_loudestAverageArtist"));

            // Execute the sort job and wait for it to finish
            success = sortJob.waitForCompletion(true);
        }

        if (success) {
            Job sortJob = Job.getInstance();
            sortJob.setJarByClass(JobRunner.class); // Ensure

            sortJob.setMapperClass(LoudestAverageArtistSortedMapper.class);
            sortJob.setReducerClass(LoudestAverageArtistSortedReducer.class);

            sortJob.setOutputKeyClass(DoubleWritable.class);
            sortJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(sortJob, new Path(args[2] + "_loudestAverageArtist"));
            FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_loudestAverageArtistSorted"));

            success = sortJob.waitForCompletion(true);
        }

        return success;

    }

    private static boolean runTopHotttnesss(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JobRunner.class);
        job.setJobName("TopHotttnesss");

        job.setMapperClass(hotttnesssSongMapper.class);
        job.setReducerClass(hotttnesssSongReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "_topHotttnesss"));

        boolean success = job.waitForCompletion(true);

        if (success) {
            Job sortJob = Job.getInstance();
            sortJob.setJarByClass(JobRunner.class);

            sortJob.setMapperClass(hotttnesssSongSortedMapper.class);
            sortJob.setReducerClass(hotttnesssSongSortedReducer.class);

            sortJob.setOutputKeyClass(DoubleWritable.class);
            sortJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(sortJob, new Path(args[2] + "_topHotttnesss"));
            FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_topHotttnesssSorted"));

            success = sortJob.waitForCompletion(true);

    }
        return success;
}


    private static boolean runTopFadeIn(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // First Job: Initial Fade Time Processing
        Job job = Job.getInstance(conf);
        job.setJarByClass(JobRunner.class);
        job.setJobName("TopFadeIn");

        job.setMapperClass(LongestFadeMapper.class);
        job.setReducerClass(LongestFadeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "_topFadeIn"));

        boolean success = job.waitForCompletion(true);

        if (success) {
            // Second Job: Combining Fade Times
            Job combinedJob = Job.getInstance(conf);
            combinedJob.setJarByClass(JobRunner.class);
            combinedJob.setJobName("TopFadeInCombined");

            combinedJob.setMapperClass(LongestFadeCombinedMapper.class);
            combinedJob.setReducerClass(LongestFadeCombinedReducer.class);

            combinedJob.setOutputKeyClass(Text.class);
            combinedJob.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(combinedJob, new Path(args[2] + "_topFadeIn"));
            FileOutputFormat.setOutputPath(combinedJob, new Path(args[2] + "_topFadeInCombined"));

            success = combinedJob.waitForCompletion(true);
        }

        if (success) {
            Job finalJob = Job.getInstance(conf);
            finalJob.setJarByClass(JobRunner.class);
            finalJob.setJobName("TopFadeInFinal");

            finalJob.setMapperClass(LongestFadeFinalMapper.class);
            finalJob.setReducerClass(LongestFadeFinalReducer.class);

            finalJob.setOutputKeyClass(Text.class);
            finalJob.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(finalJob, new Path(args[2] + "_topFadeInCombined"));
            FileOutputFormat.setOutputPath(finalJob, new Path(args[2] + "_topFadeInFinal"));

            success = finalJob.waitForCompletion(true);
        }

        return success;
    }


private static boolean runSongLongestSong(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(JobRunner.class);
    job.setJobName("SongLongestSong");

    job.setMapperClass(LongestSongsMapper.class);
    job.setReducerClass(LongestSongsReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2] + "_songLongestSong"));

    boolean success = job.waitForCompletion(true);
    if (success) {
        Job sortJob = Job.getInstance();
        sortJob.setJarByClass(JobRunner.class); // Ensure JobRunner is your main class

        // Set the new classes for the mapper and reducer
        sortJob.setMapperClass(LongestSongsCombinedMapper.class);
        sortJob.setReducerClass(LongestSongsCombinedReducer.class);

        // Set the final output key/value classes according to the reducer's outputs
        sortJob.setOutputKeyClass(DoubleWritable.class);
        sortJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortJob, new Path(args[2] + "_songLongestSong"));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_songLongestSongCombined"));

        // Execute the sort job and wait for it to finish
        success = sortJob.waitForCompletion(true);
    }
    return success;
}

private static boolean runMostEnergetic(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(JobRunner.class);
    job.setJobName("MostEnergetic");

    job.setMapperClass(MostEnergeticMapper.class);
    job.setReducerClass(MostEnergeticReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2] + "_mostEnergetic"));

    boolean success = job.waitForCompletion(true);

    if (success) {
        Job sortJob = Job.getInstance();
        sortJob.setJarByClass(JobRunner.class);

        sortJob.setMapperClass(MostEngergeticCombinedMapper.class);
        sortJob.setReducerClass(MostEngergeticCombinedReducer.class);

        sortJob.setOutputKeyClass(DoubleWritable.class);
        sortJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortJob, new Path(args[2] + "_mostEnergetic"));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_mostEnergeticCombined"));

        success = sortJob.waitForCompletion(true);
    }

    return success;
}

private static boolean runEvenHotter(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(JobRunner.class);
    job.setJobName("EvenHotter");

    job.setMapperClass(evenHotterMapper.class);
    job.setReducerClass(evenHotterReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2] + "_evenHotter"));

    boolean success = job.waitForCompletion(true);

    if (success) {
        Job sortJob = Job.getInstance();
        sortJob.setJarByClass(JobRunner.class);

        sortJob.setMapperClass(evenHotterCombinedMapper.class);
        sortJob.setReducerClass(evenHotterCombinedReducer.class);

        sortJob.setOutputKeyClass(DoubleWritable.class);
        sortJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortJob, new Path(args[2] + "_evenHotter"));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[2] + "_evenHotterCombined"));

        success = sortJob.waitForCompletion(true);
    }

    return success;

}

}

