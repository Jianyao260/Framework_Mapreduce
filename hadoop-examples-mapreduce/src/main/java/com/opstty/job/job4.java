package com.opstty.job;

import com.opstty.mapper.mapper4;
import com.opstty.reducer.reducer4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

public class job4 {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: j4 [input] [output]");
            System.exit(2);
        }
        // Job creation and description
        Job job = Job.getInstance(conf, "job4");
        job.setJobName("job4");


        // Precise Driver, Mapper and Reducer
        job.setJarByClass(job4.class);
        job.setMapperClass(mapper4.class);
        job.setReducerClass(reducer4.class);

        // Types of key/values for the mapper and the reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and output file path
        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);

        // If folder, go into all documents
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        // If output already exists, delete it
        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
