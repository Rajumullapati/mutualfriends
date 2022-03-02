**To run the assignment questions the following commands should be used.** 

**The full class name with package declaration must be mentioned if the jar is used.**


Q1: 
hadoop jar mutualfriends.jar com.hadoop.mutualfriends.MutualFriendMapReduceApp path_to mutual.txt output_path


Q2:

hadoop jar mutualfriends.jar com.hadoop.mutualfriends.MutualFriendMapperJoinMapReduceApp path_to mutual.txt path_to_userdata.txt output_path friend1_param friend2_param

Q3:

hadoop jar mutualfriends.jar com.hadoop.mutualfriends.MutualFriendReducerJoinMapReducerApp path_to mutual.txt path_to_userdata.txt output_path

Q4:

hadoop jar mutualfriends.jar com.hadoop.mutualfriends.MutualFriendInvertedIndexMapReduceApp path_to_userdata.txt output_path

Q5:

hadoop jar mutualfriends.jar com.hadoop.mutualfriends.MutualFriendCombinerMapReduceApp path_to_userdata.txt output_path








