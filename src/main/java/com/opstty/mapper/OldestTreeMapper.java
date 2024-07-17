package com.opstty.mapper;

import com.opstty.AgeDistrictWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;

public class OldestTreeMapper extends Mapper<LongWritable, Text, AgeDistrictWritable, NullWritable> {
    private int line = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (line != 0) {
            String[] columns = value.toString().split(";");
            // Ensure columns length is at least 6 to safely access columns[5] for year
            if (columns.length >= 6) {
                try {
                    int year = Integer.parseInt(columns[5].trim()); // Accessing ANNEE PLANTATION
                    String district = columns[1].trim(); // Accessing ARRONDISSEMENT
                    context.write(new AgeDistrictWritable(year, new Text(district)), NullWritable.get());
                } catch (NumberFormatException e) {
                    // Log or handle the exception (e.g., skip this record)
                    System.err.println("Failed to parse year for line: " + value.toString());
                }
            } else {
                // Handle case where columns length is not as expected
                System.err.println("Invalid format for line: " + value.toString());
            }
        }
        line++;
    }
}
