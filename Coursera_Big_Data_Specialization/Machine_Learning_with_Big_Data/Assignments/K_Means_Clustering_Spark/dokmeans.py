#A script to execute kmeans clustering in spark
#to run enter: >>> exec(open("./dokmeans.py").read())

import numpy as np
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans


#generate random data RDD we need this package
from pyspark.mllib.random import RandomRDDs

#let's generate random class data, add in a cluster center to random 2D points

#use default num of partitions, or use a definte number to make it so that the union
#  will have samples across clusters
c1_v=RandomRDDs.normalVectorRDD(sc,20,2,numPartitions=2,seed=1L).map(lambda v:np.add([1,5],v))
c2_v=RandomRDDs.normalVectorRDD(sc,16,2,numPartitions=2,seed=2L).map(lambda v:np.add([5,1],v))
c3_v=RandomRDDs.normalVectorRDD(sc,12,2,numPartitions=2,seed=3L).map(lambda v:np.add([4,6],v))

#concatenate 2 RDDs with  .union(other) function
c12    =c1_v.union(c2_v)
my_data=c12.union(c3_v)   #this now has all points, as RDD


my_kmmodel = KMeans.train(my_data,k=1,
               maxIterations=20,runs=1,
               initializationMode='k-means||',seed=10L)

#try: help(KMeans.train)  to see parameter options
#k is the number of desired clusters.
#maxIterations is the maximum number of iterations to run.
#initializationMode specifies either random initialization or initialization via k-means||.
#runs is the number of times to run the k-means algorithm (k-means is not guaranteed to find a globally optimal solution, and when run multiple times on a given dataset, the algorithm returns the best clustering result).
#initializationSteps determines the number of steps in the k-means|| algorithm.
#epsilon determines the distance threshold within which we consider k-means to have converged.
 

#type dir(my_kmmodel) to see functions available on the cluster results object

#The computeCost function might not be available on your cloudera vm,
#  spark mlllib, it computes the Sum Squared Error: my_kmmodel.computeCost(my_data)  

#This does the same thing as computeCost, and gives an example of coding a metric
#get sse of a point to the center of the cluster it's assigned to
def getsse(point):
    this_center = my_kmmodel.centers[my_kmmodel.predict(point)]
           #for this point get it's clustercenter
    return (sum([x**2 for x in (point - this_center)])) 


my_sse=my_data.map(getsse).collect()  #collect list of sse of each pt to its center
print np.array(my_sse).mean()      
 
