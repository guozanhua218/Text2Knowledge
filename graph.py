import sys
import math
import numpy
import Queue
import priodict
import heapq
import matplotlib.pyplot as plt
import matplotlib as mpl

from operator import add
from pyspark import SparkContext


#Global Variables
marked = []
edgeTo = {}
distTo = {}
path = []


#------------------------------------------------------------------------------------
def writeGephi(G,dictionary,filename):

    print '[INFO] Writing MST to Gephi'
    with open(filename + '.gml','w') as f:

        #Header
        f.write('Creator "Gijs Joost Brouwer"\n')
        f.write('graph\n')
        f.write('[\n')


        #Write Vertices
        for i,name in enumerate(dictionary):
            f.write('  node\n')
            f.write('  [\n')
            f.write('    id ' + str(i) + '\n')
            f.write('    label "' + name + '"\n')
            f.write('  ]\n')


        #Edges
        for g in G:
            for v in G[g]:
                gindex = dictionary.index(g)
                vindex = dictionary.index(v)
                f.write('  edge\n')
                f.write('  [\n')
                f.write('    source ' + str(gindex) + '\n')
                f.write('    target ' + str(vindex)  + '\n')
                f.write('    value ' + str(G[g][v]) + '\n')
                f.write('  ]\n')
       

#------------------------------------------------------
def MST(G):


    #New Graph
    newG = {}
    Q = []

    #Create Edge List From Graph
    for g in G:
        for v in G[g]:
            tuple = G[g][v],g,v
            heapq.heappush(Q,tuple)


    while (len(Q) > 0): 
    
        E = heapq.heappop(Q)
        d,v,w = E
        del marked[:]
        DFS(newG,v)
        if w not in marked:
            edge1 = {w:d}
            edge2 = {v:d}
            if (newG.has_key(v)):
                newG[v].update(edge1)
            else:
                newG[v] = edge1

            #if (gtype == 'undirected'):
            #    if (newG.has_key(w)):
            #        newG[w].update(edge2)
            #    else:
            #        newG[w] = edge2

    return newG


#------------------------------------------------------
def runMST(G):

    print '[INFO] Running Kruskal MST'
    newG = MST(G)
    return newG


#------------------------------------------------------
def DFS(G,v):


    #DFS recursive loop
    marked.append(v)
    if (G.has_key(v)):
        for w in G[v]:
            if w not in marked:
                edgeTo[w] = v
                DFS(G,w)


#------------------------------------------------------
def runDFS(G,start,end):


    print '[INFO] Depth First Search'
    DFS(G,start)

    if end in marked:
        print '[INFO] Path exists between ' + str(start) + ' and ' + str(end)
        v = end
        path.append(v)
        while (v != start):
            v = edgeTo[v]
            path.append(v)
        path.reverse()
        print '[INFO] ' + str(path)
    else:
        print '[INFO] No path exists'

    return path


#------------------------------------------------------------------------------------
def toGraph(W,dictionary,threshold,nDim):


    #Turn into Graph
    G = {}
    for v in range(nDim):
        x = W[:,v]
        for i,w in enumerate(x):
            if (w > threshold):
                key1 = dictionary[v].encode('ascii','ignore')
                key2 = dictionary[i].encode('ascii','ignore')
                print key1, key2, w
                edge = {key2:w}
                if G.has_key(key1):
                    G[key1].update(edge)
                else:
                    G[key1] = edge

    #Return
    return G



#------------------------------------------------------------------------------------
def computeW(corpus,dictionary,nItems):


    #Init
    T = numpy.zeros((nItems,1))
    W = numpy.zeros((len(corpus),nItems))


    #Count appearances
    for x in range(nItems):
        item1 = dictionary[x]
        for y,doc in enumerate(corpus):
            if item1 in doc:
                W[y,x] = 1

    
    #Total Appearances for each item
    T[:,0] = numpy.sum(W,axis=0)
    W = numpy.dot(numpy.transpose(W),W)
    W = W / len(corpus)
    T = T / len(corpus)


    #PMI
    C = numpy.dot(T,numpy.transpose(T))
    W = W / C
    for x in range(nItems):
        W[x,x] = 0


    #Return
    return W



#------------------------------------------------------------------------------------
def removeDuplicates(dictionary):

    #Init
    newdict = []


    #Find Duplicates
    for item1 in dictionary:
        exclude = 0
        for item2 in dictionary:
            if (len(item1) == len(item2) + 1):
                if (item2 in item1[:-1]):
                    exclude = 1
                    keepitem = item2
        if (len(item1) < 3):
            exclude = 1
        if (exclude == 0):
            newdict.append(item1)


    #Report back
    print 'Old Length: ' + str(len(dictionary))
    print 'New Length: ' + str(len(newdict))


    #Return
    return newdict


#------------------------------------------------------------------------------------
if __name__ == "__main__":


    #Init
    sc = SparkContext(sys.argv[1], "Python Spark Collocations Finder")



    #Read Corpus
    corpus = sc.textFile('/user/gijs/genesis/dictionary/' + sys.argv[2] + '.corpus')
    corpus = corpus.map(lambda x: x.encode('ascii','ignore'))
    corpus = corpus.collect()



    #Read dictionary
    dictionary = sc.textFile('/user/gijs/genesis/dictionary/' + sys.argv[2] + '.dict')
    dictionary = dictionary.map(lambda x: x.encode('ascii','ignore'))
    dictionary = dictionary.collect()



    #Remove Singular / Plural
    dictionary = removeDuplicates(dictionary)



    #Compute W
    nDim = 100;
    threshold = 1.5;
    W = computeW(corpus,dictionary,nDim)



    #Transform to Graph
    G = toGraph(W,dictionary,threshold,nDim)
    


    #Perform MST on Graph
    newG = runMST(G)
    


    #Write to Graph
    writeGephi(newG,dictionary,sys.argv[2])

