import sys
import math
import os

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
def toWordlist(corpus):

    wordlist = []
    for lines in corpus:
        lines = lines.encode('ascii','ingore')
        words = lines.rstrip().split(' ')
        wordlist = wordlist + words
    return wordlist


#------------------------------------------------------------------------------------
if __name__ == "__main__":


    #Init
    threshold = 0.00001
    sc = SparkContext(sys.argv[1], "Python Spark Dictionary Creater Finder")



    #Read Stopwords
    stopwords = sc.textFile('/user/gijs/genesis/dictionary/stopwords.dict')
    stopwords = stopwords.map(lambda x: (x.encode('ascii','ignore'),1))
    


    #Read Corpus
    corpus = sc.textFile('/user/gijs/genesis/dictionary/' + sys.argv[2] + '.corpus')
    corpus = corpus.collect()



    #To Word list
    wordlist = toWordlist(corpus)
    wordlist = sc.parallelize(wordlist)

    

    #Run Word Count
    dictionary = wordcount(wordlist,threshold)



    #Join with stopwords
    dictionary = dictionary.leftOuterJoin(stopwords)
    dictionary = dictionary.filter(lambda x: x[1][1] == None)
    dictionary = dictionary.map(lambda x: switchKeyValue(x))
    dictionary = dictionary.sortByKey(ascending=False)
    dictionary = dictionary.map(lambda x: switchKeyValue(x))
    dictionary = dictionary.map(lambda x: (x[0],x[1][0]))
    dictionary = dictionary.collect()



    #Write to file and put on HDFS
    with open(sys.argv[2] + '.dict','w') as f:
        for key,value in dictionary:
            print key + ',' + str(value)
            f.write(key + '\n')
    commandstr1 = 'hdfs dfs -rm -f ' + sys.argv[2] + '.dict /user/gijs/genesis/dictionary/' + sys.argv[2] + '.dict'
    commandstr2 = 'hdfs dfs -put ' + sys.argv[2] + '.dict /user/gijs/genesis/dictionary/'
    os.system(commandstr1)
    os.system(commandstr2)




    
