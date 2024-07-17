package com.opstty;

import org.apache.hadoop.io.*;

import java.io.*;

public class AgeDistrictWritable implements WritableComparable<AgeDistrictWritable> {
    private IntWritable year;
    private Text district;

    public AgeDistrictWritable() {
        this.year = new IntWritable();
        this.district = new Text();
    }

    public AgeDistrictWritable(int year, Text district) {
        this.year = new IntWritable(year);
        this.district = district;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        district.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        district.readFields(in);
    }

    @Override
    public int compareTo(AgeDistrictWritable o) {
        int cmp = year.compareTo(o.year);
        if (cmp == 0) {
            cmp = district.compareTo(o.district);
        }
        return cmp;
    }

    public IntWritable getYear() {
        return year;
    }

    public Text getDistrict() {
        return district;
    }
}
