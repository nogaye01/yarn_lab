package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;

public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private int line = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (line == 0) {
            line++;
            return;
        }

        String[] columns = value.toString().split(";");
        if (columns.length > 6) { // Assuming height is in the 7th column (index 6)
            String heightStr = columns[6].trim();
            if (!heightStr.isEmpty()) {
                try {
                    float height = Float.parseFloat(heightStr);
                    context.write(new Text(columns[2].trim()), new FloatWritable(height));
                } catch (NumberFormatException e) {
                    // Log or handle the exception (e.g., skip this record)
                    System.err.println("Failed to parse height for line: " + value.toString());
                }
            } else {
                // Handle empty height case
                System.err.println("Empty height for line: " + value.toString());
            }
        } else {
            // Handle case where columns length is not as expected
            System.err.println("Unexpected columns length for line: " + value.toString());
        }

        line++;
    }
}
