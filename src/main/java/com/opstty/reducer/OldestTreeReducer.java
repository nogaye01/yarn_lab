package com.opstty.reducer;

import com.opstty.AgeDistrictWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;

public class OldestTreeReducer extends Reducer<AgeDistrictWritable, NullWritable, Text, NullWritable> {
    @Override
    public void reduce(AgeDistrictWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // The first key in the iterable will be the one with the oldest year due to sorting
        Text district = key.getDistrict();
        context.write(district, NullWritable.get());
    }
}
