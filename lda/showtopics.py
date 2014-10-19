#!/usr/bin/python

import sys
import os
import re
import random 
import math
import urllib2
import time
import cPickle
import numpy
import onlineldavb



#---------------------------------------------------------------------
if __name__ == '__main__':


    #Init and Clear
    os.system("clear")


    #Print the topics
    vocab = str.split(file(sys.argv[1] + '.dict').read())
    testlambda = numpy.loadtxt('lambda.dat')
    for k in range(0, 10):
        lambdak = list(testlambda[k, :])
        lambdak = lambdak / sum(lambdak)
        temp = zip(lambdak, range(0, len(lambdak)))
        temp = sorted(temp, key = lambda x: x[0], reverse=True)
        print 'topic %d:' % (k)
        for i in range(0, 10):
            print '%20s  \t---\t  %.4f' % (vocab[temp[i][1]], temp[i][0])
        print

