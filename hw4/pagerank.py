import shutil
import sys
import time
from pyspark.sql import SparkSession


def parse_link(link):
    # URL string should be in this format:
    # src_id dest_id weight
    return link.split()


def s(l):
    return sum(j for _, j in l)


def main():
    # Get inputs

    file_path = sys.argv[1]
    alpha = float(sys.argv[2])  # teleport probability
    max_iterations = int(sys.argv[3])
    beta = float(sys.argv[4])  # convergence threshold

    # file_path = "./inputs/graph1.txt"
    # alpha = 0.15
    # max_iterations = 10
    # beta = 0.01

    partitions = 8

    sc = SparkSession \
        .builder \
        .appName("PageRank") \
        .getOrCreate() \
        .sparkContext

    print("Starting...")
    start = time.time()

    links = sc.textFile(file_path) \
        .map(lambda link: link.split(" ")) \
        .filter(lambda link: link[0] != link[1]) \
        .map(lambda link: (int(link[0]), (int(link[1]), float(link[2])))) \
        .repartition(partitions) \
        .cache()

    # Pre-computation

    links_by_node_id = links.groupByKey().cache()

    outgoing_edges_by_node_id = links.countByKey()

    count_nodes = links.flatMap(lambda x: (x[0], x[1][0])).distinct().count()

    # H

    h = links_by_node_id.flatMap(lambda xs: [((xs[0], x[0]), x[1] / s(list(xs[1]))) for x in xs[1]]).cache()

    getA = lambda x: 0 if outgoing_edges_by_node_id[x] > 0 else 1 / (count_nodes - 1)

    r = sc.parallelize(range(1, count_nodes + 1)).repartition(partitions).cache()

    pi1 = []
    pi = None

    for _ in range(count_nodes):
        pi1.append(1 / count_nodes)

    iteration = 0
    while iteration < max_iterations:

        # Calculate pi*H vector
        h_pi_sum = h.map(lambda x: (x[0][1], pi1[x[0][0] - 1] * x[1])).reduceByKey(lambda x, y: x + y).map(
            lambda x: (x[0], x[1] * (1 - alpha))).cache()

        pi2 = []
        # Calculate pi*A vector
        for j in range(1, count_nodes + 1):
            v = r.map(lambda i: getA(i) * pi1[i - 1] if i != j else 0).sum() * (1 - alpha)
            pi2.append((j, v))

        pi = sc.parallelize(pi2).repartition(8).cache()

        # Calculate pi * (jump probability) vector
        jump_probability = sum(pi1) * alpha / count_nodes

        # Finding pi *(S = (H+A)*(1-alpha))
        pi = h_pi_sum.union(pi).reduceByKey(lambda x, y: x + y).cache()

        # Finding pi* G
        pi = pi.map(lambda x: (x[0], x[1] + jump_probability))

        # print(pi.collect())

        # Checking precision
        norm_1 = pi.map(lambda x: abs(x[1] - pi1[x[0] - 1])).sum()

        if norm_1 < beta:
            break

        # Update value for next iteration
        pi1 = pi.values().collect()

        iteration += 1

    print("Ranking Finished in ", time.time() - start, " seconds with ", iteration + 1, " iterations")

    # Save results

    shutil.rmtree("./outputs", ignore_errors=True)

    pi.saveAsTextFile("./outputs/")


if __name__ == "__main__":
    main()
