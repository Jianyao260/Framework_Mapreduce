package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



public class mapper1 extends Mapper<LongWritable, Text,Text,NullWritable>{
    private final static IntWritable one = new IntWritable(1);
    private final Text ARRONDISSEMENT = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(";");
        for(int i=0;i< line.length;i++) {
            if (line[i].equals("")) {
                line[i] = "nan";
            }
        }
        ARRONDISSEMENT.set(line[1]);
        context.write(ARRONDISSEMENT,NullWritable.get());
    }

}
