package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mapper4 extends Mapper<LongWritable, Text, Text, NullWritable> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        IntWritable outValue = new IntWritable();
        Text outKey = new Text();
        if(key.get()!=0) {
            String[] line = value.toString().split(";");
            outValue.set((int) Float.parseFloat(line[6]));
            outKey.set(line[2]);
            context.write(outKey, outValue);
        }
    }
}
