package com.opstty.mapper;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PAIR implements WritableComparable<PAIR> {
    public String district;
    public Double height;
    public void set(String district, Double height) {
        this.district = district;
        this.height = height;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(district);
        out.writeDouble(height);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.district= in.readUTF();
        this.height = in.readDouble();

    }

    @Override
    public int compareTo(PAIR pair) {
        return 0;
    }


}