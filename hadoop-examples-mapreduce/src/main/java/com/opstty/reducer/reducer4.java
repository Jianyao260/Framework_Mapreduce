package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class reducer4 extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outValue = new IntWritable(0);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int height = 0;

        for (IntWritable Height : values) {
            if (height < Height.get())
                height = Height.get();
        }
        outValue.set(height);

        context.write(key, outValue);
    }
}
