package com.opstty.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class reducer1 extends Reducer<Text, Text,Text, NullWritable> {
        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            context.write(key,NullWritable.get());
    }
}
