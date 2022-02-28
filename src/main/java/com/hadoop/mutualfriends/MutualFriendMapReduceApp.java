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
 */
public class MutualFriendMapReduceApp {
  public static class MutualFrndMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable keyy, Text value, Context context) throws IOException, InterruptedException {

      Text frndTouple = new Text();

      String [] line = value.toString().split("\t");

      if(line.length == 2){
        int key = Integer.parseInt(line[0]);
        List<String> frnds = Arrays.asList(line[1].split(","));

        for(String friend: frnds){
          if(Integer.parseInt(friend) > key){
            frndTouple.set(key+","+friend);
          }
          else{
            frndTouple.set(friend+","+key);
          }
          context.write(frndTouple,new Text(line[1]));
        }
      }


    }
  }

  public static class MutualFrndReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
      HashSet<String> hs = new HashSet<>();
      Text res = new Text();
      StringBuilder sb = new StringBuilder();
      for(Text frndList: values){
        List<String> frnds = Arrays.asList(frndList.toString().split(","));
        for(String friend: frnds){
          if(hs.contains(friend)){
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
