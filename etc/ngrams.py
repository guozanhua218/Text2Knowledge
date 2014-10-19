import sys
import math
import numpy

from operator import add
from pyspark import SparkContext



#------------------------------------------------------------------------------------
def parseFile(strs,columns):


    mystring = ''
    mylist = strs.split('')
    badstring = 0
    for n in columns:
        if (len(mylist[n].encode('ascii','ingore')) > 0):
            mystring = mystring + ' ' + mylist[n].encode('ascii','ingore')
        else:
            badstring = 1

    if badstring == 0:
        mystring = mystring[1:]
        return mystring
    else:
        return ''
    


#------------------------------------------------------------------------------------
def switchKeyValue(tuple):

    newtuple = (tuple[1],tuple[0])
    return newtuple


#------------------------------------------------------------------------------------
def toFrequency(tuple,totalWords):

    newtuple = (tuple[0],float(tuple[1])/float(totalWords))
    newtuple = (newtuple[0],newtuple[1])
    return newtuple


#------------------------------------------------------------------------------------
def wordcount(tokens,threshold):


    #Count
    totalCount = tokens.count()


    #Map
    tokens = tokens.map(lambda x: (x,1))
    

    #Reduce
    tokens = tokens.reduceByKey(add)
    

    #Sort
    tokens = tokens.map(lambda x: switchKeyValue(x))
    tokens = tokens.sortByKey(ascending=False)
    tokens = tokens.map(lambda x: switchKeyValue(x))


    #To Frequency
    tokens = tokens.map(lambda x: toFrequency(x,totalCount))
    tokens = tokens.filter(lambda x: x[1]>threshold)


    #Return
    return tokens


#------------------------------------------------------------------------------------
def display(dictionary,nItems):


    collected = dictionary.collect()
    for x in range(nItems):
        item = collected[x]
        print item


#------------------------------------------------------------------------------------
def toWordlist(corpus,nWords):

    wordlist = []
    for lines in corpus:
        lines = lines.encode('ascii','ingore')
        words = lines.rstrip().split(' ')
        for i in range(len(words)-nWords+1):
            if (nWords == 1):
                wordlist.append(words[i])
            if (nWords == 2):
                wordlist.append(words[i] + ' ' + words[i+1]) 
            if (nWords == 3):
                wordlist.append(words[i] + ' ' + words[i+1] + ' ' + words[i+2])
            if (nWords == 4):
                wordlist.append(words[i] + ' ' + words[i+1] + ' ' + words[i+2] + ' ' + words[i+3])
    return wordlist



#------------------------------------------------------------------------------------
def pmi(bidict,unidict):


    unidict = dict(unidict)
    pmi = []
    for bigram in bidict:
        words, freq = bigram
        wordssplit = words.split(' ')
        val = 1;
        for word in wordssplit:
            val = val * unidict[word]
        if (len(wordssplit[-1]) > 2):
            pmi.append((words,math.log(freq/val,2)))
    pmi = [x for x in pmi if x[1] > 10.00]

    pmi.sort(key=lambda x: x[1])
    
    pmi.reverse()

    print pmi

#------------------------------------------------------------------------------------
if __name__ == "__main__":


    #Init
    threshold = 0.0001
    sc = SparkContext(sys.argv[1], "Python Spark Dictionary Creater Finder")



    #Read Stopwords
    stopwords = sc.textFile('/user/gijs/genesis/dictionary/stopwords.txt')
    stopwords = stopwords.map(lambda x: (x.encode('ascii','ingore'),1))



    #Read Corpus
    corpus = sc.textFile('/user/gijs/genesis/dictionary/' + sys.argv[2] + '.corpus')
    corpus = corpus.collect()



    #To Word list
    unigrams = toWordlist(corpus,1)
    bigrams = toWordlist(corpus,2)
    trigrams = toWordlist(corpus,3)
    fourgrams = toWordlist(corpus,4)



    #Parallelize
    unigrams = sc.parallelize(unigrams)
    bigrams = sc.parallelize(bigrams)
    trigrams = sc.parallelize(trigrams)
    fourgrams = sc.parallelize(fourgrams)

    

    #Run Word Count
    unidict = wordcount(unigrams,threshold)
    bidict = wordcount(bigrams,threshold)
    tridict = wordcount(trigrams,threshold)
    fourdict = wordcount(fourgrams,threshold)


    #Calculate PMI
    bidict = bidict.collect()
    unidict = unidict.collect()
    bigramPMI = pmi(bidict,unidict)



    #Join with stopwords
    #dictionary = dictionary.leftOuterJoin(stopwords)
    #dictionary = dictionary.filter(lambda x: x[1][1] == None)
    #dictionary = dictionary.map(lambda x: switchKeyValue(x))
    #dictionary = dictionary.sortByKey(ascending=False)
    #dictionary = dictionary.map(lambda x: switchKeyValue(x))
    #dictionary = dictionary.map(lambda x: (x[0],x[1][0]))
    #dictionary = dictionary.collect()



    #Write to file
    #with open(sys.argv[2] + '.dict','w') as f:
    #    for key,value in dictionary:
    #        print key + ',' + str(value)
    #        f.write(key + '\n')

    
