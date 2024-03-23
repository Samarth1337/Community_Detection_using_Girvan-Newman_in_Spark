# Community_Detection_using_Girvan-Newman_in_Spark_on_Yelp_Data

## Overview:
In this project, I explored community detection in social networks using the Yelp dataset. Leveraging Spark GraphFrames and implementing the Girvan-Newman algorithm, I aimed to detect communities of users with similar business tastes efficiently in a distributed environment.

## Problem Statement:
The task involved constructing a social network graph from the Yelp dataset, where each node represents a user, and edges represent shared business reviews. The challenge was to identify communities within this graph, which could help understand user preferences and behaviors.

## Technologies Used:

Spark GraphFrames: Utilized for efficient graph processing and community detection.

Spark RDD: Used for implementing the Girvan-Newman algorithm.

Python and Scala: Programming languages used for implementation.

JDK and Scala: Required for Spark setup and execution.

## Dataset:
I utilized the ub_sample_data.csv dataset, extracted from the Yelp review dataset. This dataset contains user_id and business_id pairs, forming the basis of the social network graph.

## Solutions Implemented:

### Task 1: Community Detection with GraphFrames
I utilized the Spark GraphFrames library to detect communities in the constructed social network graph. By implementing the Label Propagation Algorithm (LPA), I efficiently identified communities based on the underlying edge structure.

### Task 2: Community Detection with Girvan-Newman Algorithm
For this task, I implemented the Girvan-Newman algorithm using Spark RDD and standard Python/Scala libraries. This algorithm allowed me to detect communities by calculating betweenness for each edge and identifying the best communities based on modularity.

### Execution:
I executed Task 1 using the provided task1.py script, passing parameters such as filter_threshold, input_path, and output_path. Similarly, for Task 2, I utilized the task2.py script, providing arguments like filter_threshold, input_path, betweenness_output_path, and community_output_path.

## Conclusion:
This project provided hands-on experience with graph analysis and community detection in large social networks. By leveraging advanced technologies like Spark GraphFrames and implementing algorithms like Girvan-Newman, I gained insights into user behavior and preferences, which are valuable for various applications like recommendation systems and targeted marketing strategies. This project demonstrates my proficiency in big data processing, graph analysis, and algorithm implementation, making it an attractive addition to my portfolio for potential employers.
