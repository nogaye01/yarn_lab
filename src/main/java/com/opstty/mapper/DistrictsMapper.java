package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private int line = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (line != 0) {
            String[] columns = value.toString().split(";");
            // Assuming arrondissement is in the second column (index 1)
            if (columns.length > 1) {
                String district = columns[1].trim(); // Trim to remove any leading/trailing whitespace
                context.write(new Text(district), new IntWritable(1));
            }
        }
        line++;
    }
}
