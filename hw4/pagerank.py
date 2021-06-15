import os

from pyspark.sql import SparkSession


def parse_link(link):
    # URL string should be in this format:
    # src_id dest_id weight
    return link.split()


def main():
    # Get inputs

    sc = SparkSession \
        .builder \
        .appName("PageRank") \
        .getOrCreate() \
        .sparkContext

    links = sc.textFile("./inputs/graph1.txt") \
        .map(lambda link: link.split(" ")) \
        .filter(lambda link: link[0] != link[1]) \
        .map(lambda link: (int(link[0]), (int(link[1]), float(link[2])))) \
        .repartition(8) \
        .cache()

    # Pre-computation

    links_by_node_id = links.groupByKey()

    outgoing_edges_by_node_id = links.countByKey()

    count_nodes = links.flatMap(lambda x: (x[0], x[1][0])).distinct().count()

    # H

    # Save results

    try:
        os.removedirs("outputs")
    except OSError as e:
        pass

    links.map(lambda x: (x[0], list(x[1]))).saveAsTextFile("./outputs/")


if __name__ == "__main__":
    main()
