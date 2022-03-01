package com.hadoop.mutualfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MutualFriendCombinerMapReduceApp {

    public static class MutualFriendCombinerMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key+""), value);
        }

    }

    public static class MutualFriendCombinerReducer extends Reducer<Text, Text, Text, Text> {
        List<String> max_words = new ArrayList<>();
        int max_count = 0;
        private Text val_out = new Text("");
        public void reduce(Text key, Iterable<Text> value, Context context){
            int count = 0;

            for(Text t: value){
                String []  a = t.toString().split(",");
                count = count + a.length;
            }
            if(max_count == count){
                String temp = val_out.toString();
                temp = temp+"\n"+key+"\t"+max_count;
                val_out.set(temp);

            }
            else if (max_count < count){
                val_out.set(key+"\t"+max_count);
                max_words.clear();
            }

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(""),val_out);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Mutual friend: <InPath> <OutPath>");
            System.exit(2);
        }

        Configuration conf1 = new Configuration();
        conf1.set("mapred.max.split.size", (new File(args[0]).getTotalSpace()*2)+"");
        Job job = Job.getInstance(conf1, "MutualFriendCombinerMapReduceApp");
        job.setJarByClass(MutualFriendCombinerMapReduceApp.class);
        Configuration map1Conf = new Configuration(false);


        ChainMapper.addMapper(job, MutualFriendInvertedIndexMapReduceApp.MutualFriendInvertedIndexMapper.class, LongWritable.class, Text.class, Text.class, Text.class,map1Conf);
        ChainReducer.setReducer(job,MutualFriendInvertedIndexMapReduceApp.MutualFriendInvertedIndexReducer.class, Text.class, Text.class, Text.class, Text.class, map1Conf);
        ChainReducer.addMapper(job, MutualFriendCombinerMapReduceApp.MutualFriendCombinerMapper.class, LongWritable.class, Text.class, Text.class, Text.class,map1Conf);
        job.setCombinerClass(MutualFriendCombinerMapReduceApp.MutualFriendCombinerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
