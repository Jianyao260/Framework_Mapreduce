package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mapper6 extends Mapper<LongWritable, Text, Text, PAIR>{

        private final static Text outputKey = new Text();
        private PAIR outputValue = new PAIR(); //use the type of PAIR

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Split each row of data into array
            String[] line = value.toString().split(",");
            for(int i=0;i< line.length;i++) {
                if (line[i].equals("")) {
                    line[i] = "nan";
                }
            }
            this.outputKey.set(line[0]); // choose any key
            this.outputValue.set(line[2],Double.valueOf(line[8])); //district ,height
            context.write(outputKey, outputValue);
        }
}
