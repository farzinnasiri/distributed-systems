import random

import networkx as nx
import matplotlib.pyplot as plt

n = 30
p = 0.15

g = nx.generators.random_graphs.erdos_renyi_graph(n, p)

max_weight = 0
for (u, v, w) in g.edges(data=True):
    w['weight'] = random.randint(1, n//2)
    max_weight = max(int(w['weight']), max_weight)

file = open("tests/test5.txt", "a")
file.write(str(3*max_weight))
file.write("\n")

for (u, v, w) in g.edges(data=True):
    print(u, v, w['weight'])
    line = str(u) + " " + str(v) + " " + str(w['weight']) + "\n"
    file.write(line)

file.write("\n")


pos = nx.spring_layout(g)

nx.draw(g, pos, with_labels=True)
labels = nx.get_edge_attributes(g, 'weight')
nx.draw_networkx_edge_labels(g, pos, edge_labels=labels)
plt.savefig("./hw2/tests/test5.png")
