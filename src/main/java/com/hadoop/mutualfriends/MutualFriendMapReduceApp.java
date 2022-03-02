/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.hadoop.mutualfriends;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is a simple application that includes a MapReduce program.
 *
 * Q1
 */
public class MutualFriendMapReduceApp {
  public static class MutualFrndMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable keyy, Text value, Context context) throws IOException, InterruptedException {


      Text frndTouple = new Text();

      String [] line = value.toString().split("\t");

      if (line.length == 2) {

        int f1 = Integer.parseInt(line[0]);
        String[] friendsList = line[1].split(",");
        for (String friend2 : friendsList) {
         int f2 = Integer.parseInt(friend2);
          // Checks for given touple.
          if((f1==0 && f2 ==1 )||(f1==1 && f2 ==0 )||(f1==20 && f2 ==28193 )||(f1==28193 && f2 ==20 )||(f1==1 && f2 ==29826 )||(f1==29826 && f2 ==1 )||(f1==6222 && f2 ==19272 )||(f1==19272 && f2 ==6222 )||(f1==28041 && f2 == 28056)||(f1==28056 && f2 ==28041)){
            if (f1 < f2) {
              frndTouple.set(f1 + "," + f2);
            } else {
              frndTouple.set(f2 + "," + f1);
            }
            //Writes the touple along with the entire friend list.
            context.write(frndTouple, new Text(line[1]));
          }
        }
      }
    }



  }

  //Reducer class
  public static class MutualFrndReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
      HashSet<String> hs = new HashSet<>();
      Text res = new Text();
      StringBuilder sb = new StringBuilder();
      for(Text frndList: values){
        //Splits the individual friends.
        List<String> frnds = Arrays.asList(frndList.toString().split(","));
        for(String friend: frnds){
          if(hs.contains(friend)){ //Checks if is a mutual.
            sb.append(friend+",");
          }else{
            hs.add(friend);
          }
        }
      }
      if(sb.lastIndexOf(",")>-1){
        sb.deleteCharAt(sb.lastIndexOf(","));
      }
      res.set(sb.toString());
      context.write(key,res);
    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("Mutual friend: <InPath> <OutPath>");
      System.exit(2);
    }


    //Configs.
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MutualFriend");

    job.setJarByClass(MutualFriendMapReduceApp.class);
    job.setMapperClass(MutualFrndMapper.class);
    job.setReducerClass(MutualFrndReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
