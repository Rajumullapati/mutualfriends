package com.hadoop.mutualfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MutualFriendCombinerMapReduceApp {

    public static class MutualFriendCombinerMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] splits = value.toString().split("\\[");
            context.write(new Text(splits[0].trim()+""), new Text(splits[1]));
        }

    }

    public static class MutualFriendCombinerReducer extends Reducer<Text, Text, Text, Text> {
        List<String> max_words = new ArrayList<>();
        int max_count = 0;

        private Text val_out = new Text("");
        public void reduce(Text key, Iterable<Text> value, Context context){
            int count = 0;
            System.out.println("Combiner started : "+key);
            for(Text t: value){
                String []  a = t.toString().split(",");
                System.out.println(Arrays.toString(a));
                count = count + a.length;
            }
            System.out.println("count "+count);
            if(max_count == count){
                String temp = val_out.toString();
                temp = temp+"\n"+key+"\t"+max_count;
                val_out.set(temp);

            }
            else if (max_count < count){
                max_count = count;
                val_out.set(key+"\t"+max_count);
                max_words.clear();
            }

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            BufferedReader reader;
//            if(context.getConfiguration().get("USER_DATA").startsWith("/"))
//                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(String.valueOf(fs.getHomeDirectory()).substring(0,String.valueOf(fs.getHomeDirectory()).indexOf('/',9))+""+context.getConfiguration().get("USER_DATA")))));
//            else
//                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(String.valueOf(fs.getHomeDirectory())+"/"+context.getConfiguration().get("USER_DATA")))));
            System.out.println("Clean");
            context.write(new Text(""),val_out);
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Mutual friend: <InPath> <OutPath>");
            System.exit(2);
        }

        Configuration conf1 = new Configuration();
        conf1.set("mapreduce.input.fileinputformat.split.minsize", "1202020");
        conf1.set("mapreduce.task.timeout","60000000");
        Job job1 = Job.getInstance(conf1, "MutualFriendCombinerInversionMapReduceApp");
        job1.setJarByClass(MutualFriendInvertedIndexMapReduceApp.class);
        job1.setMapperClass(MutualFriendInvertedIndexMapReduceApp.MutualFriendInvertedIndexMapper.class);
        job1.setReducerClass(MutualFriendInvertedIndexMapReduceApp.MutualFriendInvertedIndexReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[0].substring(0,args[0].lastIndexOf('/'))+"/temp"));

        job1.waitForCompletion(true);
        conf1.set("INPUT_PATH",args[0].substring(0,args[0].lastIndexOf('/')));
        Job job2 = Job.getInstance(conf1, "MutualFriendCombinerMapReduceApp");
        job2.setJarByClass(MutualFriendCombinerMapReduceApp.class);
        job2.setMapperClass(MutualFriendCombinerMapReduceApp.MutualFriendCombinerMapper.class);
        job2.setCombinerClass(MutualFriendCombinerMapReduceApp.MutualFriendCombinerReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0].substring(0,args[0].lastIndexOf('/'))+"/temp/part*"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));


        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}
