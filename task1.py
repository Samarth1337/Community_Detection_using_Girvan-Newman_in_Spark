from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from graphframes import GraphFrame
import os
import time
import sys
import itertools

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell"

def find_communities(input_threshold, input_path, output_path):
    start_time = time.time()
    node_pairs = set()
    spark_conf = SparkConf().setAppName("Homework4").setMaster('local[*]')
    spark_context = SparkContext(conf=spark_conf)
    spark_context.setLogLevel("ERROR")
    sql_context = SQLContext(spark_context)
    
    rdd = spark_context.textFile(input_path)
    header = rdd.first()
    user_business = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).cache()
    user_id = user_business.map(lambda x: x[0])
    distinct_users = user_id.distinct().collect()
    vertex_set = set()
    user_map = user_business.groupByKey().collectAsMap()
    
    for combo in itertools.combinations(distinct_users, 2):
        user1, user2 = combo
        if len(set(user_map[user1]).intersection(set(user_map[user2])) ) >= input_threshold:
            node_pairs.add((user1, user2))
            node_pairs.add((user2, user1))
            vertex_set.add(user1)
            vertex_set.add(user2)
            
    vertices = sql_context.createDataFrame([(user_id,) for user_id in vertex_set], ["id"])
    edges = sql_context.createDataFrame(list(node_pairs), ["src", "dst"])
    graph = GraphFrame(vertices, edges)
    result = graph.labelPropagation(maxIter=5)
    communities = result.rdd.map(lambda x: (x[1], x[0])).groupByKey().map(lambda label: sorted(list(label[1]))).sortBy(lambda l: (len(l), l)).collect()
      
    with open(output_path, 'w') as output_file:
        for community in communities:
            output_file.write(f"'{community[0]}'")
            for i in range(1, len(community)):
                output_file.write(f", '{community[i]}'")
            output_file.write("\n")
          
    print("Duration:", time.time() - start_time)

input_threshold = int(sys.argv[1])
input_path = sys.argv[2]
output_path = sys.argv[3]

find_communities(input_threshold, input_path, output_path)
