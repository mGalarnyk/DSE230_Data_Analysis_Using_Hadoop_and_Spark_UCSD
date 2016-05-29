
# coding: utf-8

# Name: Michael Galarnyk
# Email: mgalarny@eng.ucsd.edu
# PID: A09134209
from pyspark import SparkContext
sc = SparkContext()

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip



from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel, RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils


# Read the file into an RDD
# If doing this on a real cluster, you need the file to be available on all nodes, ideally in HDFS.
path='/covtype/covtype.data'
inputRDD=sc.textFile(path)


# In[8]:

# Transform the text RDD into an RDD of LabeledPoints
Data=inputRDD.map(lambda line: [float(strip(x)) for x in line.split(',')])     .map(lambda array: LabeledPoint(array[-1], array[0:-1]))
        


# ### Making the problem binary


def counter(maybe2): 
    if maybe2 == 2.0:
        return 1.0
    else: 
        return 0.0



Label=2.0
Data=inputRDD.map(lambda line: [float(x) for x in line.split(',')])    .map(lambda V:LabeledPoint(counter(V[-1]), V[0:-1]   ))


# ### Reducing data size
# In order to see the effects of overfitting more clearly, we reduce the size of the data by a factor of 10

# In[12]:

#Data1=Data.sample(False,0.1).cache()
(trainingData,testData)=Data.randomSplit([0.7,0.3])


from time import time
errors={}
for depth in [10]:
    start=time()
    model=GradientBoostedTrees.trainClassifier(trainingData, categoricalFeaturesInfo={}, numIterations=10, maxDepth=depth, learningRate=0.27,
                                              maxBins=54)
    errors[depth]={}
    dataSets={'train':trainingData,'test':testData}
    for name in dataSets.keys():  # Calculate errors on train and test sets
        data=dataSets[name]
        Predicted=model.predict(data.map(lambda x: x.features))
        LabelsAndPredictions = data.map(lambda lp: lp.label).zip(Predicted)
        Err = LabelsAndPredictions.filter(lambda (v,p): v != p).count()/float(data.count())
        errors[depth][name]=Err
    print depth,errors[depth],int(time()-start),'seconds'
