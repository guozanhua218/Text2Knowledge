import sys
import math
import numpy

from operator import add
from pyspark import SparkContext


#------------------------------------------------------------------------------------
def writeGenesis(W,dictionary,filename):


    #Nodes
    nDim = W.shape[0]
    with open(filename + '/' + filename + '.vertices','w') as f:
        for x in range(nDim):
            f.write(str(x) + ',' + str(numpy.random.random()) + ',' + str(numpy.random.random()) + ',' + dictionary[x] + '\n')


    #Edges
    with open(filename + '/' + filename + '.edges','w') as f:
        for v in range(nDim):
            x = W[:,v]
            for i,w in enumerate(x):
                if (w > 2.0):
                    f.write(str(v) + ',' + str(i) + ',' + str(w) + '\n')
                    
                    

#------------------------------------------------------------------------------------
def writeGephi(W,dictionary,filename):


    nDim = W.shape[0]
    with open(filename + '/' + filename + '.gml','w') as f:

        #Header
        f.write('Creator "Gijs Joost Brouwer"\n')
        f.write('graph\n')
        f.write('[\n')


        #Vertices
        for x in range(nDim):
            f.write('  node\n')
            f.write('  [\n')
            f.write('    id ' + str(x) + '\n')
            f.write('    label "' + dictionary[x] + '"\n')
            f.write('  ]\n')


        #Edges
        for v in range(nDim):
            x = W[:,v]
            for i,w in enumerate(x):
                if (w > 2.0):
                    f.write('  edge\n')
                    f.write('  [\n')
                    f.write('    source ' + str(v) + '\n')
                    f.write('    target ' + str(i) + '\n')
                    f.write('    value ' + str(w) + '\n')
                    f.write('  ]\n')
        

#------------------------------------------------------------------------------------
def removeDuplicates(dictionary):

    #Init
    newdict = []


    #Remove some no-no words
    for item in dictionary:
        exclude = 0
        if item == 'overall':
            exclude = 1
        if item == 'view':
            exclude = 1
        if item == 'neurosci':
            exclude = 1
        if item == 'science':
            exclude = 1
        if (exclude == 0):
            newdict.append(item)
    dictionary = newdict
    newdict = []



    #Find Duplicates
    counter = 0
    for item1 in dictionary:
        exclude = 0
        for item2 in dictionary:
            if (len(item1) == len(item2) + 1):
                if (item2 in item1[:-1]):
                    exclude = 1
        if (len(item1) < 3):
            exclude = 1
        if (exclude == 0):
            newdict.append(item1)
        else:
            counter = counter + 1

    #Return
    print 'Old Length: ' + str(len(dictionary))
    print 'New Length: ' + str(len(newdict))
    return newdict


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
if __name__ == "__main__":


    #Init
    sc = SparkContext(sys.argv[1], "Python Spark Collocations Finder")


    #Read Corpus
    corpus = sc.textFile('/user/gijs/genesis/dictionary/' + sys.argv[2] + '.txt')
    corpus = corpus.collect()


    #Read dictionary
    dictionary = sc.textFile('/user/gijs/genesis/dictionary/' + sys.argv[2] + '.dict')
    dictionary = dictionary.collect()


    #Remove Singular / Plural
    dictionary = removeDuplicates(dictionary)


    #Compute W
    W = computeW(corpus,dictionary,200)


    #Write to Graph
    writeGephi(W,dictionary,sys.argv[2])


    #Write to Graph
    writeGenesis(W,dictionary,sys.argv[2])

