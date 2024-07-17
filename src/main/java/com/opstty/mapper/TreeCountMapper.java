package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private int line = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (line != 0) {
            String[] columns = value.toString().split(";");
            // Assuming species is in the third column (index 2), adjust the index as needed
            if (columns.length > 2) {
                context.write(new Text(columns[2]), one);
            }
        }
        line++;
    }
}



