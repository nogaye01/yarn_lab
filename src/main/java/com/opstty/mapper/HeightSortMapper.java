package com.opstty.mapper;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;

public class HeightSortMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
    private int line = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (line != 0) {
            String[] columns = value.toString().split(";");
            // Ensure columns length is sufficient and height column is not empty
            if (columns.length > 0 && !columns[6].isEmpty()) {
                try {
                    float height = Float.parseFloat(columns[6]);
                    String species = columns[3]; // Assuming species is in the 4th column (index 3)
                    context.write(new FloatWritable(height), new Text(species));
                } catch (NumberFormatException e) {
                    // Log or handle the exception (e.g., skip this record)
                    System.err.println("Failed to parse height for line: " + value.toString());
                }
            } else {
                // Handle case where columns length is not as expected or height is empty
                System.err.println("Invalid format or empty height for line: " + value.toString());
            }
        }
        line++;
    }
}
