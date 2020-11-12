package com.opstty.reducer;

import com.opstty.mapper.PAIR;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class reducer6 extends Reducer<Text, PAIR, Text, IntWritable> {

        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values,
                           final Context context) throws IOException, InterruptedException {
            String specie = key.toString();
          /*  for (Text t : values) {
                String s = t.toString();
                int height = Integer.parseInt(s);
            }*/

            outKey.set(specie);
            outValue.set(height);
            context.write(outKey, outValue);
        }
    }
}
