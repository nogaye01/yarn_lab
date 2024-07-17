package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private int line = 0;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (line != 0) {
            String[] columns = value.toString().split(";");
            // Assuming species is in the third column (index 2), adjust the index as needed
            if (columns.length > 2) {
                context.write(new Text(columns[2]), NullWritable.get());
            }
        }
        line++;
    }
}

