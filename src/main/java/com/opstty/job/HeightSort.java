package com.opstty.job;

import com.opstty.mapper.HeightSortMapper;
import com.opstty.reducer.HeightSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HeightSort {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HeightSortDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort Tree Heights");
        job.setJarByClass(HeightSort.class);
        job.setMapperClass(HeightSortMapper.class);
        job.setReducerClass(HeightSortReducer.class);

        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

