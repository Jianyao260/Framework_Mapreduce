package com.opstty.reducer;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer3 extends Reducer<Text, IntWritable, Text, IntWritable>{

        private final IntWritable outputValue = new IntWritable();
    @Override
        protected void reduce(final Text key, final Iterable<IntWritable> values,final Context context) throws IOException, InterruptedException {

            //Used to store the number of occurrences of the tree, which is equivalent to calculating the number
            int specieSum = 0;

            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                specieSum += iterator.next().get();
            }

            this.outputValue.set(specieSum);
            context.write(key,outputValue);
        }
}