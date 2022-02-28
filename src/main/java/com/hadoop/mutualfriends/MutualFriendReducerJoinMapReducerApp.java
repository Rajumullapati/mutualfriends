package com.hadoop.mutualfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
                    String dob = hm.get(frnd);
                    int age = 0;
                    try {
                        age = getAgeFromDob(dob);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if(age<min_age){
                        min_age = age;
                    }
                }
            }

            context.write(key,new Text("\t"+min_age));
        }

        private int getAgeFromDob(String dob) throws ParseException {
            Date d = new SimpleDateFormat("MM/dd/yyyy").parse(dob);
            Period period = Period.between(d.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), LocalDate.now());
            return period.getYears();

        }

        @Override
        public void setup(Reducer.Context context) throws IOException {
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

}
