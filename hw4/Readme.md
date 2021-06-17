# Page Rank: Distributed Systems2021-SUT

In this home work, we were supposed to implement a simple version of the  [page rank](https://en.wikipedia.org/wiki/PageRank) algorithm using [Apache Spark](https://spark.apache.org/) in the [pyspark](https://spark.apache.org/docs/latest/api/python/) environment.

You can run a test of the code with the following command in the codes directory:

    python3 pagerank.py ./inputs/graph1.txt 0.15 10 0.01



## Description of the solution
It's important for the algorithm to scale with the data and have a performance with large inputs. 

### Pre-computation
It is important to compute constant objects so that no extra time is wasted recomputing them. Constants like *Number of nodes* are calculated easily but some other objects need more computation!

#### Nodes and links
I needed to remove loops and store the links of each node in a good format for later use. 

    links = sc.textFile(file_path) \  
        .map(lambda link: link.split(" ")) \  
        .filter(lambda link: link[0] != link[1]) \  
        .map(lambda link: (int(link[0]), (int(link[1]), float(link[2])))) \  
        .repartition(partitions) \  
        .cache()
and this code, groups the links by their source:

    links_by_node_id = links.groupByKey().cache()
    
Also I store the number of outgoing edges for each node in a dictionary for fast access:

    outgoing_edges_by_node_id = links.countByKey()

#### H
Knowing that **H** is sparse, I only stored the non-zero elements in the following format:

    [((i,j1),v1),((i,j2),v2),((i,j3),v3), ...]

 In my implementation, this format was the the best way to access the elements if needed.

The following code converts *links* to our desired format:

    h = links_by_node_id.flatMap(lambda xs: [((xs[0], x[0]), x[1] / s(list(xs[1]))) for x in xs[1]]).cache()
    
#### Utility Functions
s(l:list) is a utility function that sum's the second value of list of tuples:`[(1,2),(1,2),...]`

    def s(l):  
        return sum(j for _, j in l)

This functions gets `A[i]` when called

    getA = lambda x: 0 if outgoing_edges_by_node_id[x] > 0 else 1 / (count_nodes - 1)

### Main Algorithm

To find the appropriate vector, the algorithm loops until either the precision is enough or max-iteration is reached. To be efficient and fast, instead of multiplying matrices in  a normal form, I first calculate each of **G** individually and then sum them together. Also for each part I try to get the desired element with minimum operations.

#### pi * H

    h_pi_sum = h.map(lambda x: (x[0][1], pi1[x[0][0] - 1] * x[1])).reduceByKey(lambda x, y: x + y).map(  
        lambda x: (x[0], x[1] * (1 - alpha))).cache()

#### pi*A

    for j in range(1, count_nodes + 1):  
        v = r.map(lambda i: getA(i) * pi1[i - 1] if i != j else 0).sum() * (1 - alpha)  
        pi2.append((j, v))

####  pi*(alpha/n)

    jump_probability = sum(pi1) * alpha / count_nodes

#### Merging results

    pi = h_pi_sum.union(pi).reduceByKey(lambda x, y: x + y).cache()


### Termination
The algorithm terminates if either the threshold is met or the max-iterations is reached:

    norm_1 = pi.map(lambda x: abs(x[1] - pi1[x[0] - 1])).sum()  
      
    if norm_1 < beta:  
        break

    while iteration < max_iterations:
	    # do stuff
At the end, the result are saved in the outputs directory.

## Performance 
To improve performance, I tried to use the **cache** feature of spark to avoid redoing computation. 

Also actions like *groupBy* do a shuffling on the data, so I tried to avoid them as much as possible. To achieve this, I **reparation** the graphs data ,after cleaning it up and reformatting. This way the data is shuffled and the each nodes links are grouped with each other(hence achieving *locality of data*), later on I also cache the grouped data.

In spark, transformations are done in a lazy matter and when actions happen they are evaluated. So most of the times I tried to put actions as the last step of computing. 

The last thing I did to improve performance was to pip the data and design the flow of computation so that there is minimum need to re-access data  and when it is needed, the operation is  `O(1)`


     