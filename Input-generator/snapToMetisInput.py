import sys
import os

'''
input : snap file

arglist:

1.metis edge list
2.input for metis

output :

1. metis format

2. edge list

3. giraph_file

'''

# fout = open('hello.txt', 'w')
# fout.write('Hello, world!\n')                # .write(str)
# fout.write('My name is Homer.\n')
# fout.write("What a beautiful day we're having.\n")
# fout.close()
#


snap_file=sys.argv[1]
metis_edgeList=sys.argv[2]
metis_graph=sys.argv[3]
giraph_graph=sys.argv[4]


snapfile = open(snap_file, 'r')
metis_edgeListfile = open(metis_edgeList, 'w')
reorder_file = open('reorder.txt', 'w')

reordering = dict()
rReordering = dict()
vid = 1
count=0

graph={}


for line in snapfile:
    if line.startswith("#"):
	    continue
    # print line[:-1]
    src= line[:-1].split()[0]
    dst= line[:-1].split()[1]

    if src == dst:
        continue

    if(rReordering.has_key(src)):
        pass
    else:
        reordering[vid] = src
        rReordering[src] = vid
        reorder_file.write(str(vid) + ' ' + str(src) + '\n')
        vid=vid+1
        


    if(rReordering.has_key(dst)):
        pass
    else:
        reordering[vid] = dst
        rReordering[dst] = vid
        reorder_file.write(str(vid) + ' ' + str(dst) + '\n')
        vid=vid+1

    # graph formation

    if(graph.has_key(rReordering[src])):
        n=graph[rReordering[src]]
        if not rReordering[dst] in n:
            count += 1
            n.add(rReordering[dst])
            graph[rReordering[src]]=n

    else:
        n = set()
        n.add(rReordering[dst])
        count += 1
        graph[rReordering[src]]=n

    # To make it undirected
    if(graph.has_key(rReordering[dst])):
        n=graph[rReordering[dst]]
        n.add(rReordering[src])
        graph[rReordering[dst]]=n

    else:
        n = set()
        n.add(rReordering[src])
        graph[rReordering[dst]]=n

    
    # print rReordering[src],rReordering[dst]
  #  outString=str(rReordering[src]-1)+" "+str(rReordering[dst]-1)
  #  outString1=str(rReordering[dst]-1)+" "+str(rReordering[src]-1)
    # print outString
#    metis_edgeListfile.write(outString+"\n")
#    metis_edgeListfile.write(outString1+"\n")   

# print count
# print rReordering
# print graph
snapfile.close()
#metis_edgeListfile.close()


metis_graphfile = open(metis_graph, 'w')
giraph_graphfile = open(giraph_graph, 'w')
gml_graphfile = open("gml.txt", 'w')

numV = vid - 1
numE=count

metis_graphfile.write(str(numV)+" "+str(numE)+"\n")
gml_graphfile.write("graph [\ndirected 1\nvertex_properties [\n]\nedge_properties [\n]\n")
edge_count_gml = 1

# print "Vertices" + str(numV)
for i in range(1,numV+1):

    if graph.has_key(i):
    	metis_graphfile.write( " ".join( str(v) for v in list(graph[i])) + "\n")
        for adjVertex in graph[i]:
            metis_edgeListfile.write(str(i) + " " + str(adjVertex) + "\n")
    else:
	    metis_graphfile.write("\n")

    # giraph_graphfile.write(str(i)+" "+" ".join( str(v) for v in list(graph[i])) + "\n")
    
"""
    outstr=str(i)+"\t"+str(0.0)
    prefix="\t"
    if graph.has_key(i):
        for v in list(graph[i]):
            outstr=outstr+prefix+str(v)+prefix+str(1.0)
        giraph_graphfile.write(outstr+"\n")
    else:
        giraph_graphfile.write(outstr+"\n")

    
    gml_graphfile.write("node [\n")
    gml_graphfile.write("id " + str(i) + "\n")
    gml_graphfile.write("]\n")
    for adjVertex in graph[i]:
        gml_graphfile.write("edge [\n")
        gml_graphfile.write("id " + str(edge_count_gml) + "\n")
        gml_graphfile.write("source " + str(i) + "\n")
        gml_graphfile.write("target " + str(adjVertex) + "\n")
        gml_graphfile.write("]\n")
        edge_count_gml += 1
    """
gml_graphfile.write("]")
metis_graphfile.close()
metis_edgeListfile.close()
giraph_graphfile.close()
gml_graphfile.close()
