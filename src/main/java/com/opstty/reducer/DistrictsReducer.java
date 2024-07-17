package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // Sum up all the values (which are counts of trees)
        for (IntWritable value : values) {
            sum += value.get();
        }
        // Output the district and its total count of trees
        context.write(key, new IntWritable(sum));
    }
}
