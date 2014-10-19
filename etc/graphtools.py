import numpy
import scipy
import sys
import Queue
import priodict
import heapq
import matplotlib.pyplot as plt
import matplotlib as mpl

#Global Variables
marked = []
edgeTo = {}
distTo = {}
path = []
gtype = 'undirected'


#------------------------------------------------------
def readGraph(filename):

	#New Graph (dictionary object)
	G = {}
	X = []
	nodenames = []


	#Read Vertices
	print '[INFO] Reading graph from file'
	with open(filename + '/' + filename + '.vertices','r') as f:
		for line in f:
			elements = line.rstrip().split(',')
			nodenames.append(elements[3])
			X.append((float(elements[1]),float(elements[2])))
	X = numpy.asarray(X)


	#Read Edges
	with open(filename + '/' + filename + '.edges','r') as f:
		for line in f:
			elements = line.rstrip().split(',')
			i = elements[0]
			j = elements[1]
			w = 1.000/(float(elements[2]))
			edge = {j:w}
			if (G.has_key(i)):
				G[i].update(edge)
			else:
				G[i] = edge

		print G

	#Return
	return (G,nodenames,X)
	


#------------------------------------------------------------------------------------
def writeGephi(G,dictionary,filename):

	print '[INFO] Writing MST to Gephi'

	
    # nDim = W.shape[0]
	with open(filename + '/' + filename + '.gml','w') as f:

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
				f.write('  edge\n')
				f.write('  [\n')
				f.write('    source ' + str(g) + '\n')
				f.write('    target ' + str(v) + '\n')
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

			if (gtype == 'undirected'):
				if (newG.has_key(w)):
					newG[w].update(edge2)
				else:
					newG[w] = edge2

	return newG


#------------------------------------------------------
def runMST(G):

	print '[INFO] Running Kruskal MST'
	newG = MST(G)
	return newG


#------------------------------------------------------
def centrality(G):

	print '[INFO] Computing Centrality'
	for g in G:
		print g



#------------------------------------------------------
def Dijkstra(G,start,end=None):


	#Init	
	D = {}	#Dctionary of final distances
	P = {}	#Dictionary of predecessors
	Q = priodict.priorityDictionary()
	Q[start] = 0



	#While there are elements in the priority queue	
	for v in Q:
		D[v] = Q[v]
		if v == end:
			break
		for w in G[v]:
			vwLength = D[v] + G[v][w]
			if w in D:
				if vwLength < D[w]:
					raise ValueError
 			elif w not in Q or vwLength < Q[w]:
				Q[w] = vwLength
				P[w] = v
	
	return (D,P)


#------------------------------------------------------
def runDijkstra(G,start,end):


	#Run Dijkstra
	print '[INFO] Running Dijkstra Algorithm'
	D,P = Dijkstra(G,start,end)
	
	#Create Path	
	path = []
	while 1:
		path.append(end)
		if end == start:
			break
		if end not in P:
			path = []
			break
		end = P[end]
	path.reverse()
	if (len(path) > 0):
		print '[INFO] ' + str(path)
	else:
		print '[INFO] No path exists'

	#Return
	return path


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


#------------------------------------------------------
def runBFS(G,start,end):


	#Init variables
	print '[INFO] Breadth First Search'
	Q = Queue.Queue()
	marked.append(start)
	for i in G:
		distTo[i] = float('inf')
	distTo[start] = 0
	Q.put(start)


	#Run BFS
	while not Q.empty():
		v = Q.get()
		for w in G[v]:
			if w not in marked:
				edgeTo[w] = v
				distTo[w] = distTo[v] + 1
				marked.append(w)
				Q.put(w)


	#Get the path
	if end in marked:
		print '[INFO] Path exists between ' + str(start) + ' and ' + str(end)
		v = end
		path.append(v)
		while (distTo[v] > 0):
			v = edgeTo[v]
			path.append(v)
		path.reverse()
		print '[INFO] ' + str(path)
	else:
		print '[INFO] No path exists'

	return path

#-------------------------------------------------------
def plotGraph(G,X):


	print '[INFO] Plot Graph'
	mpl.rcParams['toolbar'] = 'None'

	#Draw nodes and edges
	for g in G:
		for v in G[g]:
			g = int(g)
			v = int(v)
			line = plt.plot([X[g][0],X[v][0]],[X[g][1],X[v][1]],'k',zorder=1)
			plt.setp(line,linewidth=1)
	for i in range(X.shape[0]):
		if str(i) in G:
			plt.scatter(X[i,0], X[i,1], 150, c=(0.25,0.25,0.25),alpha=1,zorder=2)
		else:
			plt.scatter(X[i,0], X[i,1], 150, c=(0.5,0.5,0.5),alpha=1,zorder=2)


#------------------------------------------------------
def plotPath(P,G,X):


	for i in xrange(0,len(P)-1):
		vs = int(P[i])
		ve = int(P[i+1])
		line = plt.plot([X[vs][0],X[ve][0]],[X[vs][1],X[ve][1]],'r',zorder=3)
		plt.setp(line,linewidth=3)


#------------------------------------------------------
def plotMST(G,X):

	#Draw nodes and edges
	for g in G:
		for v in G[g]:
			g = int(g)
			v = int(v)
			line = plt.plot([X[g][0],X[v][0]],[X[g][1],X[v][1]],'r')
			plt.setp(line,linewidth=3)
	plt.scatter(X[:,0], X[:,1], 150, c='k',alpha=1)


#------------------------------------------------------
if __name__ == '__main__':


	#Init
	print '[INFO] Graph Tools'
		

	#Check Number of arguments
	if (len(sys.argv) != 2):
		print '[INFO] Not the right number of input arguments'
		exit(0)


	#Read the graph from file
	G,nodenames,X = readGraph(sys.argv[1])

	print nodenames

	#Plot Graph
	plotGraph(G,X)


	#1. Calculate Shortests paths between all nodes.
	#2. Rank nodes based on their presence in these shortests paths
	#centrality(G)


	#P = runDijkstra(G,'10','26')
	#plotPath(P,G,X)


	#Perform MST on Graph
	newG = runMST(G)
	plotMST(newG,X)


	#Write Tree to Gephi Graph File
	writeGephi(newG,nodenames,sys.argv[1])


	#Show Plot
	plt.show()
