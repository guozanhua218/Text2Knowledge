#!/usr/bin/python

import string
import numpy
import sys
import re
import os
import onlineldavb


#---------------------------------------------------------------------
def grabAbstracts(pages,batchsize,pageID):

    #Init
    docset = []
    pagenames = []

    #Grab Random pages    
    indices = numpy.random.choice(len(pages),batchsize)
    for index in indices:
        docset.append(pages[index])
        pagenames.append(str(pageID[index]))

    #Return
    return list(docset), pagenames, pages, pageID


#---------------------------------------------------------------------
if __name__ == '__main__':

    
    #Init
    batchsize = 64
    K = 40
    os.system("clear")


    #Our vocabulary
    vocab = file(sys.argv[1] + '.dict').readlines()
    vocab = [item.rstrip() for item in vocab]
    W = len(vocab)


    #Read in Abstracts
    pages = file(sys.argv[1] + '.corpus').readlines()
    pages = [strs.rstrip() for strs in pages]
    D = len(pages)

    pageID = range(0,D)
    nBatches = D / batchsize


    #Initialize the algorithm with alpha=1/K, eta=1/K, tau_0=1024, kappa=0.7
    lda = onlineldavb.OnlineLDA(vocab, K, D, 1./K, 1./K, 128., 0.7)


    #Run
    nBatches = 100;
    for iteration in range(0, nBatches):

        #Grab Abstracts
        (docset, pagenames, pages,pageID) = grabAbstracts(pages,batchsize,pageID)

        #Give them to online LDA
        (gamma, bound) = lda.update_lambda(docset)
      
        #Compute an estimate of held-out perplexity
        (wordids, wordcts) = onlineldavb.parse_doc_list(docset, lda._vocab)
        perwordbound = bound * len(docset) / (D * sum(map(sum, wordcts)))
        print '%d:  rho_t = %f,  held-out perplexity estimate = %f' % (iteration, lda._rhot, numpy.exp(-perwordbound))

        #Save to file
        if (iteration % 10 == 0):
            numpy.savetxt('lambda.dat', lda._lambda)
            numpy.savetxt('gamma.dat', gamma)


