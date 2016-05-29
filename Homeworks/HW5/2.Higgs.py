
# coding: utf-8

# Name: Michael Galarnyk    
# Email: mgalarny@eng.ucsd.edu  
# PID: A09134209
from pyspark import SparkContext
sc = SparkContext()

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip
from pyspark.mllib.util import MLUtils
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel



path='/HIGGS/HIGGS.csv'
inputRDD=sc.textFile(path)


Data=inputRDD.map(lambda line: [float(strip(x)) for x in line.split(',')]).map(lambda a: LabeledPoint(a[0], a[1:]))

Data1=Data.sample(False,0.1, seed=255).cache()
(trainingData,testData)=Data1.randomSplit([0.7,0.3], seed=255)

from time import time
errors={}
depth = 10
start=time()
model=GradientBoostedTrees.trainClassifier(trainingData, categoricalFeaturesInfo={}, numIterations=10, maxDepth=depth,
    learningRate=0.25)
errors[depth]={}
dataSets={'train':trainingData,'test':testData}
for name in dataSets.keys():  # Calculate errors on train and test sets
    data=dataSets[name]
    Predicted=model.predict(data.map(lambda x: x.features))

    LabelsAndPredictions = data.map(lambda lp: lp.label).zip(Predicted)
    Err = LabelsAndPredictions.filter(lambda (v,p): v != p).count()/float(data.count())
    errors[depth][name]=Err
print depth,errors[depth],int(time()-start),'seconds'