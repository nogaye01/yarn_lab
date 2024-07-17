package com.opstty.job;

import com.opstty.mapper.TreeCount2Mapper;
import com.opstty.mapper.MaxTreeDistrictMapper;
import com.opstty.reducer.TreeCount2Reducer;
import com.opstty.reducer.MaxTreeDistrictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTreeDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Phase 1: Count trees per district
        Job job1 = Job.getInstance(conf, "Tree Count per District");
        job1.setJarByClass(MaxTreeDistrict.class);
        job1.setMapperClass(TreeCount2Mapper.class);
        job1.setCombinerClass(TreeCount2Reducer.class); // Optional combiner to optimize
        job1.setReducerClass(TreeCount2Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp-output"));
        job1.waitForCompletion(true);

        // Phase 2: Find district with most trees
        Job job2 = Job.getInstance(conf, "District with Most Trees");
        job2.setJarByClass(MaxTreeDistrict.class);
        job2.setMapperClass(MaxTreeDistrictMapper.class);
        job2.setReducerClass(MaxTreeDistrictReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("temp-output"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
