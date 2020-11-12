package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class mapper5 extends Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        IntWritable outKey = new IntWritable();
        Text outValue = new Text();

        if(key.get()!=0) {
            String[] line = value.toString().split(";");
            outKey.set((int) Float.parseFloat(line[6]));
            outValue.set(line[2]);
            context.write(outKey,outValue);
        }
    }
}