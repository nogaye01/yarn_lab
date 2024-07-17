package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTreeDistrictReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String maxDistrict = null;
        int maxTrees = 0;

        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            String district = parts[0];
            int count = Integer.parseInt(parts[1]);

            if (count > maxTrees) {
                maxTrees = count;
                maxDistrict = district;
            }
        }

        context.write(new Text(maxDistrict), new IntWritable(maxTrees));
    }
}
