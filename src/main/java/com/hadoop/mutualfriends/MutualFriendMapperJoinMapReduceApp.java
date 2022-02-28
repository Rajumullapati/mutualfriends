package com.hadoop.mutualfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class MutualFriendMapperJoinMapReduceApp {
    public static class MutualFrndMapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String,String> hm = new HashMap<>();
        @Override
        public void map(LongWritable keyy, Text value, Context context) throws IOException, InterruptedException {
            Text frndTouple = new Text();

            String [] line = value.toString().split("\t");

            if(line.length == 2){
                int key = Integer.parseInt(line[0]);
                List<String> frnds = Arrays.asList(line[1].split(","));
                StringBuffer sb = new StringBuffer();
                for(String friend: frnds){
                    sb.append(hm.get(friend)+",");
                }

                if(sb.lastIndexOf(",")>-1)
                    sb.deleteCharAt(sb.lastIndexOf(","));

                for(String friend: frnds){
                    if(Integer.parseInt(friend) > key){
                        frndTouple.set(key+","+friend);
                    }
                    else{
                        frndTouple.set(friend+","+key);
                    }
                    context.write(frndTouple,new Text(sb.toString()));
                }
            }
        }

        @Override
        public void setup(Context context) throws IOException {
            URI[] files = context.getCacheFiles();
            for(URI file: files){
                if(file.getPath().equals(context.getConfiguration().get("USER_DATA"))){
                    BufferedReader reader = new BufferedReader(new FileReader(file.getPath()));
                    String line = "";
                    while ((line = reader.readLine()) != null)
                    {
                        String[] words = line.split(",");
                        hm.put(words[0],words[9]);
                    }
                }
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
                        int d = Integer.parseInt(dob.substring(6));
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
        if (args.length != 3) {
            System.err.println("Mutual friend mapper join: <InPath> <UserData> <OutPath>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("USER_DATA",args[1]);
        Job job = Job.getInstance(conf, "MutualFriend");
        job.addCacheFile(new URI(args[1]));


    }
}
