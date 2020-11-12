package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class reducer5 extends Reducer<IntWritable, Text, IntWritable, Text> {


    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text Values : values) {

            context.write(key, Values);

        }

    }
}

