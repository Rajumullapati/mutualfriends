package com.hadoop.mutualfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.*;

public class MutualFriendReducerJoinMapReducerApp {
    public static class MutualFrndReducerJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable keyy, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if(line.length==1) {
                return;
            }
            context.write(new Text(line[0]),new Text(line[1]));
        }
    }

    public static class MutualFrndMapJoinReducer extends Reducer<Text, Text, Text, Text> {

        HashMap<String, String> hm = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            int min_age = Integer.MAX_VALUE;

            for(Text dobList: values){

                List<String> frnds = Arrays.asList(dobList.toString().split(","));
                
                for(String frnd: frnds){
                    String dob = hm.get(frnd); // Gets age of each friends.
                    int age = 0;
                    try {
                        age = getAgeFromDob(dob);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if(age<min_age){
                        min_age = age; // Gets the min age.
                    }
                }
            }

            context.write(key,new Text("\t"+min_age));
        }

        //Utility to find the age in years.
        private int getAgeFromDob(String dob) throws ParseException {
            Date d = new SimpleDateFormat("MM/dd/yyyy").parse(dob);
            Period period = Period.between(d.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), LocalDate.now());
            return period.getYears();

        }

        @Override
        public void setup(Reducer.Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader;
            //Reads the userdata file.
            if(context.getConfiguration().get("USER_DATA").startsWith("/"))
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(String.valueOf(fs.getHomeDirectory()).substring(0,String.valueOf(fs.getHomeDirectory()).indexOf('/',9))+""+context.getConfiguration().get("USER_DATA")))));
            else
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(String.valueOf(fs.getHomeDirectory())+"/"+context.getConfiguration().get("USER_DATA")))));
            String line ="";
            while ((line = reader.readLine()) != null)
            {
                String[] words = line.split(",");
                hm.put(words[0],words[9]);
            }
        }

    }

    public static void main(String [] args) throws  Exception{
        if (args.length != 3) {
            System.err.println("Mutual friend mapper join: <InPath> <UserData> <OutPath>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("USER_DATA",args[1]); //Adds user data file path to job configs.
        Job job = Job.getInstance(conf, "MutualFriendReducerJoinMapReducerApp");


        job.setJarByClass(MutualFriendReducerJoinMapReducerApp.class);
        job.setMapperClass(MutualFriendReducerJoinMapReducerApp.MutualFrndReducerJoinMapper.class);
        job.setReducerClass(MutualFriendReducerJoinMapReducerApp.MutualFrndMapJoinReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
