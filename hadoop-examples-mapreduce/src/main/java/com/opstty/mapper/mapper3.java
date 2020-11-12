package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mapper3 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable outputValue = new IntWritable(0);
        private Text outputKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Split each row of data ino artrays
            String[] line = value.toString().split(";");
            for(int i=0;i< line.length;i++) {
                if (line[i].equals("")) {
                    line[i] = "nan";
                }
            }
            this.outputKey.set(line[2]);
            context.write(outputKey, outputValue);
        }
    }

