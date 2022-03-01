package com.hadoop.mutualfriends;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MutualFriendInvertedIndexMapReduceApp {

    public static class MutualFriendInvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        public static int count = 1;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();

            String split[] = val.split(",");

            for(String str: split){
                context.write(new Text(str), new Text(count+""));
                count++;
            }

        }
    }

    public static class MutualFriendInvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            List<Integer> arr = new ArrayList<Integer>();
            for(Text val : value){
                arr.add(Integer.parseInt(val.toString()));
            }
            Collections.sort(arr);
            context.write(key, new Text("\t"+arr.toString()));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Mutual friend: <InPath> <OutPath>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        conf.set("mapred.max.split.size", (new File(args[0]).getTotalSpace()*2)+"");
        Job job = Job.getInstance(conf, "MutualFriendInvertedIndexMapReduceApp");

        job.setJarByClass(MutualFriendInvertedIndexMapReduceApp.class);
        job.setMapperClass(MutualFriendInvertedIndexMapReduceApp.MutualFriendInvertedIndexMapper.class);
        job.setReducerClass(MutualFriendInvertedIndexMapReduceApp.MutualFriendInvertedIndexReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
