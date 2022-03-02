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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


public class MutualFriendMapperJoinMapReduceApp {
    private static Logger log = LoggerFactory.getLogger(MutualFriendMapperJoinMapReduceApp.class);
    public static class MutualFrndMapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {


        private HashMap<String,String> hm = new HashMap<>();
        @Override
        public void map(LongWritable keyy, Text value, Context context) throws IOException, InterruptedException {
            Text frndTouple = new Text();
            String f1 = context.getConfiguration().get("F1");
            String f2 = context.getConfiguration().get("F2");
            String [] line = value.toString().split("\t");

            if(line.length == 2){
                int key = Integer.parseInt(line[0]);

                if(!(line[0].equalsIgnoreCase(f1) || line[0].equalsIgnoreCase(f2))){
                    return;
                }
                List<String> frnds = Arrays.asList(line[1].split(","));
                StringBuffer sb = new StringBuffer();
                for(String friend: frnds){
                    sb.append(hm.get(friend)+",");
                }

                if(sb.lastIndexOf(",")>-1)
                    sb.deleteCharAt(sb.lastIndexOf(","));

                for(String friend: frnds){
                    if(!(friend.equalsIgnoreCase(f1) || friend.equalsIgnoreCase(f2))){
                        return;
                    }
                    if(Integer.parseInt(friend) > key){
                        frndTouple.set(key+","+friend);
                    }
                    else{
                        frndTouple.set(friend+","+key);
                    }

                    System.out.println(frndTouple);
                    context.write(frndTouple,new Text(sb.toString()));
                }
            }
        }

        @Override
        public void setup(Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader;
            if(context.getConfiguration().get("USER_DATA").startsWith("/"))
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(String.valueOf(fs.getHomeDirectory()).substring(0,String.valueOf(fs.getHomeDirectory()).indexOf('/',9))+""+context.getConfiguration().get("USER_DATA")))));
            else
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(String.valueOf(fs.getHomeDirectory())+"/"+context.getConfiguration().get("USER_DATA")))));
//            BufferedReader reader = new BufferedReader(new FileReader(context.getConfiguration().get("USER_DATA")));
            String line = "";
            while ((line = reader.readLine()) != null)
            {
                String[] words = line.split(",");
                hm.put(words[0],words[9]);
            }
        }

    }

    public static class MutualFrndMapJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> hs = new HashSet<>();
            Text res = new Text();
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            int count = 0;
            for(Text dobList: values){

                List<String> dobs = Arrays.asList(dobList.toString().split(","));
                for(String dob: dobs){
                    if(hs.contains(dob)){
                        int d = Integer.parseInt(dob.substring(dob.lastIndexOf("/")+1));
                        sb.append(dob+",");
                        if(d>1995){
                            count++;
                        }
                    }else{
                        hs.add(dob);
                    }
                }
            }
            if(sb.lastIndexOf(",")>-1){
                sb.deleteCharAt(sb.lastIndexOf(","));
            }
            sb.append("]");
            res.set(sb +"\t"+count);
            context.write(key,res);
        }
    }

    public static void main(String [] args) throws  Exception{
        if (args.length != 5) {
            System.err.println("Mutual friend mapper join: <InPath> <UserData> <OutPath> <Friend1> <Friend2>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("USER_DATA",args[1]);
        conf.set("F1",args[3]);
        conf.set("F2",args[4]);
        Job job = Job.getInstance(conf, "MutualFriendMapperJoinMapReduceApp");

        job.setJarByClass(MutualFriendMapperJoinMapReduceApp.class);
        job.setMapperClass(MutualFriendMapperJoinMapReduceApp.MutualFrndMapJoinMapper.class);
        job.setReducerClass(MutualFriendMapperJoinMapReduceApp.MutualFrndMapJoinReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
