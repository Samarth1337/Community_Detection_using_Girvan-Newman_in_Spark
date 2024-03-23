from pyspark import SparkConf, SparkContext
from collections import defaultdict
import sys, os, time, itertools

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell"

def find_communities_gn(filter_threshold, input_path, betweenness_output_path, community_output_path):
    def girvan_newman_algorithm(root_node, vertices, graph):
        tree = {0: root_node}
        path_num = {root_node: 1}
        node_level = graph[root_node]
        child = {root_node: node_level}
        level = 1
        parents = defaultdict(set)
        traversed = set([root_node])

        for i in node_level:
            parents[i].add(root_node)

        while node_level:
            tree[level] = node_level
            level += 1

            for n in node_level:
                traversed.add(n)

            next_level = set()

            for n in node_level:
                new_node = set()

                for new in graph[n]:
                    if new not in traversed:
                        new_node.add(new)
                        parents[new].add(n)
                        next_level.add(new)

                child[n] = new_node
                curr_node_parents = parents[n]
                path_num[n] = 0

                if curr_node_parents:
                    path_num[n] = sum(path_num[parent] for parent in curr_node_parents)
                else:
                    path_num[n] = 1

            node_level = next_level

        node_credit, edge_contributions = defaultdict(float), {}

        for i in vertices:
            node_credit[i] = 1

        while level > 1:
            for i in tree[level - 1]:
                total = path_num[i]

                for parent in parents[i]:
                    edge = tuple(sorted((i, parent)))
                    edge_contributions[edge] = node_credit[i] * (path_num[parent] / total)
                    node_credit[parent] += edge_contributions[edge]

            level -= 1

        return [(k, i) for k, i in edge_contributions.items()]

    def calculate_modularity(communities, m, A, deg):
        Q = 0.0

        for comm in communities:
            currQ = 0.0

            for i in comm:
                for j in comm:
                    currQ += A[(i, j)] - (deg[i] * deg[j]) / (2 * m)

            Q += currQ

        return Q / (2 * m)

    def get_all_communities(vertices, graph):
        communities, traversed = [], set()

        for i in vertices:
            if i in traversed:
                continue

            traversed.add(i)
            current_node = graph[i]
            current_community = [i]

            if not current_node:
                communities.append(current_community)
                continue

            while current_node:
                next_node = set()

                for n in current_node:
                    if n not in traversed:
                        next_node.add(n)
                        traversed.add(n)
                        current_community.append(n)
                        node_next = graph[n]

                        for next_n in node_next:
                            if next_n not in traversed:
                                next_node.add(next_n)

                current_node = next_node

            communities.append(current_community)

        return communities

    def best_community(vertices, edges, graph, vertex_rdd):
        deg, best_community, bestQ, m, edges_left = {}, [], -1, len(edges) // 2, len(edges) // 2

        for k, v in graph.items():
            deg[k] = len(v)

        A = defaultdict(float)

        for edge in edges:
            A[(edge[0], edge[1])] = 1
            A[(edge[1], edge[0])] = 1

        while edges_left:
            betweenness = vertex_rdd.map(lambda vertex: girvan_newman_algorithm(vertex, vertices, graph)).flatMap(lambda x: [p for p in x]) \
                .reduceByKey(lambda a, b: a + b).map(lambda x: (x[0], x[1] / 2)).sortBy(lambda x: (-x[1], x[0])).collect()

            top = betweenness[0][1]

            for l in betweenness:
                if l[1] == top:
                    node1, node2 = l[0][0], l[0][1]
                    graph[node1].remove(node2)
                    graph[node2].remove(node1)
                    edges_left -= 1
                else:
                    break

            current_community = get_all_communities(vertices, graph)
            currentQ = calculate_modularity(current_community, m, A, deg)

            if currentQ > bestQ:
                bestQ = currentQ
                best_community = current_community

        return best_community

    start = time.time()
    conf = SparkConf().setAppName("Homework4").setMaster('local[*]')
    spark_context = SparkContext(conf=conf)
    spark_context.setLogLevel("ERROR")
    rdd = spark_context.textFile(input_path)
    vertex_set, edge_set = set(), set()
    header = rdd.first()
    user_business = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).cache()
    user_id = user_business.map(lambda x: x[0])
    distinct_users = user_id.distinct().collect()
    user_map = user_business.groupByKey().map(lambda x: (x[0], list(x[1]))).collectAsMap()

    # Find vertices and edges that meet the filter threshold
    for combo in itertools.combinations(distinct_users, 2):
        if len(set(user_map[combo[0]]).intersection(set(user_map[combo[1]]))) >= filter_threshold:
            vertex_set.add(combo[0])
            vertex_set.add(combo[1])
            edge_set.add((combo[1], combo[0]))
            edge_set.add(combo)

    # Create a graph from vertices and edges
    graph = defaultdict(set)

    for combo in edge_set:
        graph[combo[0]].add(combo[1])

    # Convert vertices to an RDD for parallel processing
    vertex_rdd = spark_context.parallelize(vertex_set)

    # Execute Girvan-Newman Algorithm to calculate betweenness
    betweenness = vertex_rdd.map(lambda vertex: girvan_newman_algorithm(vertex, vertex_set, graph)).flatMap(lambda x: [c for c in x]) \
        .reduceByKey(lambda a, b: a + b).map(lambda x: (x[0], x[1] / 2)).sortBy(lambda x: (-x[1], x[0])).collect()

    with open(betweenness_output_path, 'w') as f:
        for c in betweenness:
            f.write(f"('{c[0][0]}', '{c[0][1]}'),{c[1]}\n")

    # Find the best communities using Girvan-Newman Algorithm
    comm_list = best_community(vertex_set, edge_set, graph, vertex_rdd)
    best_out_comms = spark_context.parallelize(comm_list).map(lambda x: sorted(x)).sortBy(lambda x: (len(x), x)).collect()

    # Write community results to output file
    with open(community_output_path, 'w') as f:
        for commu in best_out_comms:
            f.write(f"'{commu[0]}'")
            for i in range(1, len(commu)):
                f.write(f", '{commu[i]}'")
            f.write("\n")

    print("Duration:", time.time() - start)

# Parsing command line arguments
filter_threshold = int(sys.argv[1])
input_path = sys.argv[2]
betweenness_output_path = sys.argv[3]
community_output_path = sys.argv[4]

# Calling the function to find communities
find_communities_gn(filter_threshold, input_path, betweenness_output_path, community_output_path)
